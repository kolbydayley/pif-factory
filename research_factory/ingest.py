from __future__ import annotations

import datetime as dt
import mimetypes
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from . import db
from .paths import corpus_dir
from .sources import load_source_list
from .text import TranscriptQuality, analyze_transcript_quality, segment_text, transcript_to_text
from .util import dumps_json, loads_json, now_iso, parse_date, parse_datetime, read_text, sha256_text, slugify, stable_id, write_text_atomic


PODCAST_NS = "{https://podcastindex.org/namespace/1.0}"
ITUNES_NS = "{http://www.itunes.com/dtds/podcast-1.0.dtd}"
ATOM_NS = "{http://www.w3.org/2005/Atom}"

SOURCE_KIND_TO_METHOD = {
    "creator_provided_rss_transcript": "rss",
    "official_show_transcript": "official_page",
    "youtube_captions": "youtube_caption",
    "voyager_transcription": "voyager_transcription",
}

SOURCE_KIND_FOUND_STATUS = {
    "creator_provided_rss_transcript": "rss_transcript_found",
    "official_show_transcript": "official_page_found",
    "youtube_captions": "youtube_caption_found",
    "voyager_transcription": "voyager_transcription_completed",
}

TRANSCRIPTION_ELIGIBLE_STATUS = "transcription_eligible"


def fetch_url(url: str) -> tuple[str, str | None]:
    if url.startswith("file://"):
        path = Path(urllib.request.url2pathname(url[7:]))
        return path.read_text(encoding="utf-8"), mimetypes.guess_type(path.name)[0]
    if "://" not in url:
        path = Path(url).expanduser().resolve()
        return path.read_text(encoding="utf-8"), mimetypes.guess_type(path.name)[0]
    req = urllib.request.Request(url, headers={"User-Agent": "podcast-intelligence-factory/0.1"})
    with urllib.request.urlopen(req, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        body = response.read().decode(charset, errors="replace")
        return body, response.headers.get_content_type()


def _youtube_video_id(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host.endswith("youtu.be"):
        video_id = parsed.path.strip("/").split("/")[0]
        if video_id:
            return video_id
    if "youtube.com" in host:
        if parsed.path == "/watch":
            video_id = parse_qs(parsed.query).get("v", [None])[0]
            if video_id:
                return video_id
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 2 and parts[0] in {"embed", "shorts", "live"}:
            return parts[1]
    raise ValueError(f"Unsupported YouTube URL: {url}")


def fetch_transcript_source(url: str, *, source_kind: str, transcript_type: str | None) -> tuple[str, str | None]:
    if source_kind != "youtube_captions":
        return fetch_url(url)
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except ImportError as exc:
        raise RuntimeError("youtube-transcript-api is required for youtube_captions sources") from exc
    video_id = _youtube_video_id(url)
    fetched = YouTubeTranscriptApi().fetch(video_id, languages=("en",))
    text = "\n".join(snippet.text.strip() for snippet in fetched if snippet.text.strip())
    return text, transcript_type or "text/plain"


def record_transcript_acquisition_attempt(
    conn,
    *,
    episode_id: str,
    method: str,
    status: str,
    result_url: str | None = None,
    result_source_kind: str | None = None,
    official_public: bool = False,
    policy_allowed: bool = True,
    error_class: str | None = None,
    notes: str | None = None,
    worker_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    episode = conn.execute("SELECT id, source_id FROM episodes WHERE id = ?", (episode_id,)).fetchone()
    if not episode:
        raise ValueError(f"Episode not found: {episode_id}")
    ts = now_iso()
    attempt_index = int(
        conn.execute(
            "SELECT COUNT(*) AS count FROM transcript_acquisition_attempts WHERE episode_id = ?",
            (episode_id,),
        ).fetchone()["count"]
        or 0
    ) + 1
    attempt_id = stable_id(episode_id, method, status, result_url or "", error_class or "", ts, str(attempt_index), prefix="ta_")
    eligible = 1 if status == TRANSCRIPTION_ELIGIBLE_STATUS else 0
    conn.execute(
        """
        INSERT INTO transcript_acquisition_attempts
          (
            id, episode_id, source_id, method, status, result_url, result_source_kind,
            official_public, policy_allowed, error_class, notes, worker_id, metadata_json, created_at
          )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            attempt_id,
            episode_id,
            episode["source_id"],
            method,
            status,
            result_url,
            result_source_kind,
            1 if official_public else 0,
            1 if policy_allowed else 0,
            error_class,
            notes,
            worker_id,
            dumps_json(metadata or {}),
            ts,
        ),
    )
    conn.execute(
        """
        INSERT INTO transcript_acquisition_status
          (episode_id, source_id, status, last_method, attempts_count, eligible_for_transcription, updated_at)
        VALUES (?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT(episode_id) DO UPDATE SET
          status = excluded.status,
          source_id = excluded.source_id,
          last_method = excluded.last_method,
          attempts_count = transcript_acquisition_status.attempts_count + 1,
          eligible_for_transcription = excluded.eligible_for_transcription,
          updated_at = excluded.updated_at
        """,
        (episode_id, episode["source_id"], status, method, eligible, ts),
    )
    return {
        "attempt_id": attempt_id,
        "episode_id": episode_id,
        "method": method,
        "status": status,
        "eligible_for_transcription": bool(eligible),
    }


def mark_transcript_exhausted(
    conn,
    *,
    episode_id: str,
    worker_id: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    return record_transcript_acquisition_attempt(
        conn,
        episode_id=episode_id,
        method="browser_discovery",
        status=TRANSCRIPTION_ELIGIBLE_STATUS,
        policy_allowed=True,
        notes=notes or "Online public transcript discovery exhausted; episode is eligible for configured audio transcription fallback.",
        worker_id=worker_id,
    )


def record_fetch_transcript_failure(
    conn,
    *,
    episode_id: str,
    source_kind: str,
    error: str,
    worker_id: str | None = None,
) -> dict[str, Any]:
    method = SOURCE_KIND_TO_METHOD.get(source_kind, source_kind)
    status = "fetch_failed"
    error_class = "fetch_failed"
    lowered = error.lower()
    if source_kind == "youtube_captions" and (
        "blocking requests" in lowered
        or "requestblocked" in lowered
        or "ip" in lowered and "block" in lowered
    ):
        status = "youtube_caption_blocked"
        error_class = "youtube_caption_blocked"
    return record_transcript_acquisition_attempt(
        conn,
        episode_id=episode_id,
        method=method,
        status=status,
        result_source_kind=source_kind,
        policy_allowed=True,
        error_class=error_class,
        notes=error[:1000],
        worker_id=worker_id,
    )


def record_transcript_source_found(
    conn,
    *,
    episode_id: str,
    source_kind: str,
    result_url: str,
    worker_id: str | None = None,
) -> dict[str, Any]:
    return record_transcript_acquisition_attempt(
        conn,
        episode_id=episode_id,
        method=SOURCE_KIND_TO_METHOD.get(source_kind, source_kind),
        status=SOURCE_KIND_FOUND_STATUS.get(source_kind, "transcript_source_found"),
        result_url=result_url,
        result_source_kind=source_kind,
        official_public=source_kind in {"creator_provided_rss_transcript", "official_show_transcript", "youtube_captions"},
        policy_allowed=True,
        worker_id=worker_id,
    )


def enqueue_transcription_jobs(
    conn,
    *,
    lane: str,
    provider: str,
    label_pack: str,
    limit: int,
    priority: int = 40,
    dry_run: bool = False,
) -> dict[str, Any]:
    if limit < 1:
        raise ValueError("--limit must be at least 1")
    rows = conn.execute(
        """
        SELECT
          episodes.id AS episode_id,
          episodes.title AS episode_title,
          episodes.audio_url,
          episodes.published_at,
          sources.name AS source_name,
          transcript_acquisition_status.status AS acquisition_status,
          transcript_acquisition_status.attempts_count
        FROM transcript_acquisition_status
        JOIN episodes ON episodes.id = transcript_acquisition_status.episode_id
        JOIN sources ON sources.id = episodes.source_id
        WHERE transcript_acquisition_status.eligible_for_transcription = 1
          AND NOT EXISTS (
            SELECT 1
            FROM transcripts
            WHERE transcripts.episode_id = episodes.id
              AND transcripts.status = 'ready'
          )
          AND NOT EXISTS (
            SELECT 1
            FROM jobs
            WHERE jobs.lane = ?
              AND jobs.job_type = 'transcribe_audio'
              AND jobs.target_id = episodes.id
              AND jobs.status IN ('pending', 'claimed')
          )
        ORDER BY episodes.published_at DESC, transcript_acquisition_status.updated_at ASC
        LIMIT ?
        """,
        (lane, limit),
    ).fetchall()
    selected = []
    skipped = []
    enqueued = 0
    for row in rows:
        item = dict(row)
        if not row["audio_url"]:
            skipped.append({**item, "skip_reason": "missing_audio_url"})
            continue
        selected.append(item)
        if dry_run:
            continue
        job_id = db.enqueue_job(
            conn,
            lane=lane,
            job_type="transcribe_audio",
            target_id=row["episode_id"],
            payload={"provider": provider, "label_pack": label_pack, "source_kind": "voyager_transcription"},
            priority=priority,
            max_attempts=1,
        )
        if job_id:
            enqueued += 1
    if not dry_run:
        conn.commit()
    return {
        "ok": True,
        "dry_run": dry_run,
        "provider": provider,
        "label_pack": label_pack,
        "matched": len(rows),
        "selected": len(selected),
        "enqueued": enqueued,
        "skipped": skipped,
        "episodes": selected[:100],
    }


def enqueue_sources(conn, source_list: str | Path, *, lane: str, since: str | None, label_pack: str) -> dict[str, int]:
    sources = load_source_list(source_list)
    since_dt = parse_date(since)
    stats = {"sources": 0, "episodes": 0, "transcript_jobs": 0, "missing_transcripts": 0, "source_errors": 0}
    for source in sources:
        source_id = upsert_source(conn, source)
        conn.commit()
        stats["sources"] += 1
        rss_url = source.get("rss_url")
        if not rss_url:
            continue
        try:
            feed_text, _ = fetch_url(str(rss_url))
            episodes = parse_feed(feed_text, source_id=source_id)
        except Exception as exc:
            db.enqueue_job(
                conn,
                lane=lane,
                job_type="source_fetch_failed",
                target_id=source_id,
                payload={"rss_url": str(rss_url), "error": _short_error(exc)},
                priority=750,
                max_attempts=1,
            )
            stats["source_errors"] += 1
            conn.commit()
            continue
        complete_diagnostic_jobs(conn, lane=lane, job_type="source_fetch_failed", target_id=source_id)
        for episode in episodes:
            published = parse_datetime(episode.get("published_at"))
            if since_dt and published and published < since_dt:
                continue
            episode_id = upsert_episode(conn, episode)
            stats["episodes"] += 1
            if episode.get("transcript_url"):
                record_transcript_source_found(
                    conn,
                    episode_id=episode_id,
                    source_kind="creator_provided_rss_transcript",
                    result_url=str(episode["transcript_url"]),
                )
                complete_diagnostic_jobs(conn, lane=lane, job_type="manual_transcript_required", target_id=episode_id)
                job_id = db.enqueue_job(
                    conn,
                    lane=lane,
                    job_type="fetch_transcript",
                    target_id=episode_id,
                    payload={"label_pack": label_pack},
                    priority=30,
                )
                if job_id:
                    stats["transcript_jobs"] += 1
            else:
                db.enqueue_job(
                    conn,
                    lane=lane,
                    job_type="manual_transcript_required",
                    target_id=episode_id,
                    payload={"reason": "No creator-provided RSS transcript link found."},
                    priority=500,
                    max_attempts=1,
                )
                record_transcript_acquisition_attempt(
                    conn,
                    episode_id=episode_id,
                    method="rss",
                    status="browser_discovery_pending",
                    notes="No creator-provided RSS transcript link found; public transcript discovery required before transcription fallback.",
                )
                stats["missing_transcripts"] += 1
        conn.commit()
    conn.commit()
    return stats


def _short_error(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text[:1000]


def complete_diagnostic_jobs(conn, *, lane: str, job_type: str, target_id: str) -> None:
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
        WHERE lane = ?
          AND job_type = ?
          AND target_id = ?
          AND status IN ('pending', 'claimed', 'failed')
        """,
        (ts, ts, lane, job_type, target_id),
    )


def upsert_source(conn, source: dict[str, Any]) -> str:
    ts = now_iso()
    source_id = source.get("id") or slugify(source["name"])
    conn.execute(
        """
        INSERT INTO sources
          (id, name, rss_url, homepage_url, category, policy, transcript_policy, enabled, metadata_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          name = excluded.name,
          rss_url = excluded.rss_url,
          homepage_url = excluded.homepage_url,
          category = excluded.category,
          policy = excluded.policy,
          transcript_policy = excluded.transcript_policy,
          enabled = excluded.enabled,
          metadata_json = excluded.metadata_json,
          updated_at = excluded.updated_at
        """,
        (
            source_id,
            source["name"],
            source.get("rss_url"),
            source.get("homepage_url"),
            source.get("category"),
            source.get("policy", "private_analysis_only"),
            source.get("transcript_policy", "creator_rss_transcripts_only"),
            1 if source.get("enabled", True) else 0,
            dumps_json({k: v for k, v in source.items() if k not in {"id", "name", "rss_url", "homepage_url"}}),
            ts,
            ts,
        ),
    )
    return source_id


def parse_feed(feed_text: str, *, source_id: str) -> list[dict[str, Any]]:
    root = ET.fromstring(feed_text.encode("utf-8"))
    items = root.findall(".//item") or root.findall(f".//{ATOM_NS}entry")
    episodes: list[dict[str, Any]] = []
    for item in items:
        title = _text(item, "title") or _text(item, f"{ATOM_NS}title") or "Untitled episode"
        guid = _text(item, "guid") or _text(item, f"{ATOM_NS}id") or _text(item, "link") or title
        link = _text(item, "link") or _attr(item, f"{ATOM_NS}link", "href")
        description = _text(item, "description") or _text(item, f"{ITUNES_NS}summary") or _text(item, "summary")
        published_at = _text(item, "pubDate") or _text(item, "published") or _text(item, f"{ATOM_NS}published")
        audio_url = None
        for enclosure in item.findall("enclosure"):
            if (enclosure.get("type") or "").startswith("audio") or enclosure.get("url"):
                audio_url = enclosure.get("url")
                break
        transcript_url, transcript_type = _transcript_link(item)
        episode_id = stable_id(source_id, guid, prefix="ep_")
        episodes.append(
            {
                "id": episode_id,
                "source_id": source_id,
                "guid": guid,
                "title": title.strip(),
                "description": (description or "").strip(),
                "url": link,
                "audio_url": audio_url,
                "published_at": parse_datetime(published_at).isoformat() if parse_datetime(published_at) else published_at,
                "duration_seconds": _duration_seconds(_text(item, f"{ITUNES_NS}duration")),
                "transcript_url": transcript_url,
                "transcript_type": transcript_type,
            }
        )
    return episodes


def upsert_episode(conn, episode: dict[str, Any]) -> str:
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO episodes
          (
            id, source_id, guid, title, description, url, audio_url, published_at, duration_seconds,
            feed_transcript_url, feed_transcript_type,
            verified_transcript_url, verified_transcript_type, verified_transcript_source_kind,
            transcript_url, transcript_type, created_at, updated_at
          )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id, guid) DO UPDATE SET
          title = excluded.title,
          description = excluded.description,
          url = excluded.url,
          audio_url = excluded.audio_url,
          published_at = excluded.published_at,
          duration_seconds = excluded.duration_seconds,
          feed_transcript_url = excluded.feed_transcript_url,
          feed_transcript_type = excluded.feed_transcript_type,
          transcript_url = COALESCE(episodes.verified_transcript_url, excluded.feed_transcript_url, episodes.transcript_url),
          transcript_type = COALESCE(episodes.verified_transcript_type, excluded.feed_transcript_type, episodes.transcript_type),
          updated_at = excluded.updated_at
        """,
        (
            episode["id"],
            episode["source_id"],
            episode["guid"],
            episode["title"],
            episode.get("description"),
            episode.get("url"),
            episode.get("audio_url"),
            episode.get("published_at"),
            episode.get("duration_seconds"),
            episode.get("transcript_url"),
            episode.get("transcript_type"),
            None,
            None,
            None,
            episode.get("transcript_url"),
            episode.get("transcript_type"),
            ts,
            ts,
        ),
    )
    row = conn.execute("SELECT id FROM episodes WHERE source_id = ? AND guid = ?", (episode["source_id"], episode["guid"])).fetchone()
    return row["id"]


def fetch_and_segment_transcript(conn, episode_id: str, *, label_pack: str, lane: str, source_kind: str = "creator_provided_rss_transcript") -> dict[str, int | str]:
    episode = conn.execute("SELECT * FROM episodes WHERE id = ?", (episode_id,)).fetchone()
    if not episode:
        raise ValueError(f"Episode not found: {episode_id}")
    if not episode["transcript_url"]:
        raise ValueError(f"Episode has no creator-provided transcript URL: {episode_id}")
    body, content_type = fetch_transcript_source(
        episode["transcript_url"],
        source_kind=source_kind,
        transcript_type=episode["transcript_type"],
    )
    text = transcript_to_text(body, content_type or episode["transcript_type"], episode["transcript_url"])
    record_transcript_source_found(
        conn,
        episode_id=episode_id,
        source_kind=source_kind,
        result_url=episode["transcript_url"],
    )
    return store_transcript_text(
        conn,
        episode_id=episode_id,
        text=text,
        source_kind=source_kind,
        source_url=episode["transcript_url"],
        content_type=content_type or episode["transcript_type"],
        label_pack=label_pack,
        lane=lane,
    )


def store_transcript_text(
    conn,
    *,
    episode_id: str,
    text: str,
    source_kind: str,
    source_url: str,
    content_type: str | None,
    label_pack: str,
    lane: str,
) -> dict[str, int | str]:
    episode = conn.execute("SELECT * FROM episodes WHERE id = ?", (episode_id,)).fetchone()
    if not episode:
        raise ValueError(f"Episode not found: {episode_id}")
    if len(text.split()) < 20:
        raise ValueError(f"Transcript too short after parsing: {episode_id}")
    quality = analyze_transcript_quality(text, content_type=content_type or episode["transcript_type"], source_url=source_url)
    base_transcript_id = stable_id(episode_id, source_url, prefix="tr_")
    sha = sha256_text(text)
    existing = conn.execute("SELECT raw_text_sha256 FROM transcripts WHERE id = ?", (base_transcript_id,)).fetchone()
    transcript_id = base_transcript_id
    if existing and existing["raw_text_sha256"] != sha:
        labeled_segments = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM labels
            JOIN segments ON segments.id = labels.segment_id
            WHERE segments.transcript_id = ?
            """,
            (base_transcript_id,),
        ).fetchone()["count"]
        if labeled_segments:
            transcript_id = stable_id(episode_id, source_url, sha[:16], prefix="tr_")
    transcript_path = corpus_dir() / "transcripts" / f"{transcript_id}.txt"
    write_text_atomic(transcript_path, text)
    ts = now_iso()
    word_count = len(text.split())
    policy_json = _transcript_policy_json(quality)
    status = "quarantined" if quality.quarantine else "ready"
    conn.execute(
        """
        INSERT INTO transcripts
          (id, episode_id, source_kind, source_url, content_type, raw_text_path, raw_text_sha256, status, fetched_at, policy_json, word_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          content_type = excluded.content_type,
          raw_text_path = excluded.raw_text_path,
          raw_text_sha256 = excluded.raw_text_sha256,
          status = excluded.status,
          fetched_at = excluded.fetched_at,
          policy_json = excluded.policy_json,
          word_count = excluded.word_count,
          updated_at = excluded.updated_at
        """,
        (
            transcript_id,
            episode_id,
            source_kind,
            source_url,
            content_type or episode["transcript_type"],
            str(transcript_path.relative_to(corpus_dir().parent)),
            sha,
            status,
            ts,
            dumps_json(policy_json),
            word_count,
            ts,
            ts,
        ),
    )
    if quality.quarantine:
        removed = quarantine_transcript_segments(conn, transcript_id, reason="; ".join(quality.issues), quality=quality)
        return {"transcript_id": transcript_id, "status": "quarantined", "segments": 0, "label_jobs": 0, "removed_segments": removed["removed_segments"], "removed_label_jobs": removed["removed_label_jobs"], "quality_issues": ",".join(quality.issues)}
    preparation = None
    segment_source_text = text
    if label_pack in {"ai_discourse_v2", "ai_discourse_v3", "ai_discourse_v3_1"}:
        from .prep import prepare_transcript, prepared_text_for_transcript

        preparation = prepare_transcript(conn, transcript_id, force=True)
        prepared_text, _ = prepared_text_for_transcript(conn, transcript_id)
        if prepared_text and len(prepared_text.split()) >= 20:
            segment_source_text = prepared_text
    segment_count = 0
    label_jobs = 0
    segments = segment_text(segment_source_text)
    for segment in segments:
        segment_id = stable_id(transcript_id, str(segment.index), str(segment.start_char), prefix="seg_")
        segment_path = corpus_dir() / "segments" / f"{segment_id}.txt"
        write_text_atomic(segment_path, segment.text)
        conn.execute(
            """
            INSERT INTO segments
              (id, transcript_id, episode_id, source_id, segment_index, start_char, end_char, text_path, text_sha256, word_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(transcript_id, segment_index) DO UPDATE SET
              start_char = excluded.start_char,
              end_char = excluded.end_char,
              text_path = excluded.text_path,
              text_sha256 = excluded.text_sha256,
              word_count = excluded.word_count
            """,
            (
                segment_id,
                transcript_id,
                episode_id,
                episode["source_id"],
                segment.index,
                segment.start_char,
                segment.end_char,
                str(segment_path.relative_to(corpus_dir().parent)),
                sha256_text(segment.text),
                segment.word_count,
                ts,
            ),
        )
        row = conn.execute(
            "SELECT id FROM segments WHERE transcript_id = ? AND segment_index = ?",
            (transcript_id, segment.index),
        ).fetchone()
        actual_segment_id = row["id"] if row else segment_id
        segment_count += 1
        job_id = db.enqueue_job(
            conn,
            lane=lane,
            job_type="label_segment",
            target_id=actual_segment_id,
            payload={
                "label_pack": label_pack,
                **(
                    {
                        "transcript_preparation_id": preparation["id"],
                        "transcript_artifact_type": preparation["artifact_type"],
                        "source_quality_score": preparation["quality_score"],
                    }
                    if preparation
                    else {}
                ),
            },
            priority=100,
        )
        if job_id:
            label_jobs += 1
    removed_stale_segments = _cleanup_stale_segments(conn, transcript_id, {segment.index for segment in segments})
    if removed_stale_segments:
        result = {"transcript_id": transcript_id, "segments": segment_count, "label_jobs": label_jobs, "removed_stale_segments": removed_stale_segments}
        if preparation:
            result["preparation_id"] = preparation["id"]
            result["artifact_type"] = preparation["artifact_type"]
        return result
    result = {"transcript_id": transcript_id, "segments": segment_count, "label_jobs": label_jobs}
    if preparation:
        result["preparation_id"] = preparation["id"]
        result["artifact_type"] = preparation["artifact_type"]
    return result


def _transcript_policy_json(quality: TranscriptQuality) -> dict[str, Any]:
    return {
        "private_analysis_only": True,
        "do_not_publish_full_text": True,
        "quality": {
            "ok": quality.ok,
            "quarantine": quality.quarantine,
            "issues": quality.issues,
            "metrics": quality.metrics,
        },
    }


def scan_transcript_quality(conn, *, limit: int | None = None, include_quarantined: bool = False) -> list[dict[str, Any]]:
    sql = """
        SELECT id, raw_text_path, content_type, source_url, status, policy_json
        FROM transcripts
        WHERE (? OR status != 'quarantined')
        ORDER BY updated_at DESC, id ASC
    """
    params: list[Any] = [1 if include_quarantined else 0]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
    else:
        rows = conn.execute(sql, params).fetchall()
    findings: list[dict[str, Any]] = []
    for row in rows:
        path = corpus_dir().parent / row["raw_text_path"]
        if not path.exists():
            continue
        text = read_text(path)
        quality = analyze_transcript_quality(text, content_type=row["content_type"], source_url=row["source_url"])
        if not quality.quarantine:
            continue
        counts = _transcript_segment_counts(conn, row["id"])
        findings.append(
            {
                "transcript_id": row["id"],
                "status": row["status"],
                "issues": quality.issues,
                "metrics": quality.metrics,
                **counts,
            }
        )
    return findings


def quarantine_contaminated_transcripts(conn, *, apply: bool = False, limit: int | None = None, include_quarantined: bool = False) -> dict[str, Any]:
    findings = scan_transcript_quality(conn, limit=limit, include_quarantined=include_quarantined)
    changed = []
    for finding in findings:
        if not apply:
            continue
        row = conn.execute(
            "SELECT id, raw_text_path, content_type, source_url, policy_json FROM transcripts WHERE id = ?",
            (finding["transcript_id"],),
        ).fetchone()
        if not row:
            continue
        text = read_text(corpus_dir().parent / row["raw_text_path"])
        quality = analyze_transcript_quality(text, content_type=row["content_type"], source_url=row["source_url"])
        result = quarantine_transcript_segments(conn, row["id"], reason="; ".join(quality.issues), quality=quality)
        changed.append({"transcript_id": row["id"], **result})
    if apply:
        conn.commit()
    return {"ok": True, "apply": apply, "found": len(findings), "changed": changed, "findings": findings[:100]}


def quarantine_transcript_segments(conn, transcript_id: str, *, reason: str, quality: TranscriptQuality) -> dict[str, int]:
    ts = now_iso()
    transcript = conn.execute("SELECT policy_json FROM transcripts WHERE id = ?", (transcript_id,)).fetchone()
    policy = loads_json(transcript["policy_json"], {}) if transcript else {}
    policy.update(_transcript_policy_json(quality))
    policy["quarantine_reason"] = reason
    segment_rows = conn.execute(
        """
        SELECT segments.id, segments.text_path,
               EXISTS(SELECT 1 FROM labels WHERE labels.segment_id = segments.id) AS has_label
        FROM segments
        WHERE transcript_id = ?
        """,
        (transcript_id,),
    ).fetchall()
    segment_ids = [row["id"] for row in segment_rows]
    removed_label_jobs = 0
    removed_segments = 0
    if segment_ids:
        placeholders = ", ".join("?" for _ in segment_ids)
        removed_label_jobs = conn.execute(
            f"""
            DELETE FROM jobs
            WHERE job_type = 'label_segment'
              AND status IN ('pending', 'claimed', 'failed')
              AND target_id IN ({placeholders})
            """,
            segment_ids,
        ).rowcount
    unlabeled_rows = [row for row in segment_rows if not row["has_label"]]
    if unlabeled_rows:
        unlabeled_ids = [row["id"] for row in unlabeled_rows]
        placeholders = ", ".join("?" for _ in unlabeled_ids)
        conn.execute(f"DELETE FROM content_spans WHERE legacy_segment_id IN ({placeholders})", unlabeled_ids)
        removed_segments = conn.execute(f"DELETE FROM segments WHERE id IN ({placeholders})", unlabeled_ids).rowcount
        for row in unlabeled_rows:
            try:
                (corpus_dir().parent / row["text_path"]).unlink()
            except FileNotFoundError:
                pass
    if segment_ids:
        placeholders = ", ".join("?" for _ in segment_ids)
        conn.execute(
            f"""
            UPDATE labels
            SET needs_review = 1,
                status = CASE WHEN status = 'ready' THEN 'quarantined_needs_review' ELSE status END
            WHERE segment_id IN ({placeholders})
            """,
            segment_ids,
        )
    conn.execute(
        """
        UPDATE transcripts
        SET status = 'quarantined',
            policy_json = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (dumps_json(policy), ts, transcript_id),
    )
    conn.execute(
        """
        UPDATE content_artifacts
        SET status = 'quarantined',
            policy_json = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (dumps_json(policy), ts, f"ca_{transcript_id}"),
    )
    return {"removed_segments": int(removed_segments), "removed_label_jobs": int(removed_label_jobs)}


def _transcript_segment_counts(conn, transcript_id: str) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS segments,
          SUM(CASE WHEN EXISTS(SELECT 1 FROM labels WHERE labels.segment_id = segments.id) THEN 1 ELSE 0 END) AS labeled_segments,
          SUM(CASE WHEN EXISTS(SELECT 1 FROM jobs WHERE jobs.target_id = segments.id AND jobs.job_type = 'label_segment' AND jobs.status = 'pending') THEN 1 ELSE 0 END) AS pending_label_jobs
        FROM segments
        WHERE transcript_id = ?
        """,
        (transcript_id,),
    ).fetchone()
    return {
        "segments": int(row["segments"] or 0),
        "labeled_segments": int(row["labeled_segments"] or 0),
        "pending_label_jobs": int(row["pending_label_jobs"] or 0),
    }


def _cleanup_stale_segments(conn, transcript_id: str, valid_indexes: set[int]) -> int:
    if not valid_indexes:
        return 0
    placeholders = ", ".join("?" for _ in valid_indexes)
    rows = conn.execute(
        f"""
        SELECT id
        FROM segments
        WHERE transcript_id = ?
          AND segment_index NOT IN ({placeholders})
          AND NOT EXISTS (
            SELECT 1
            FROM labels
            WHERE labels.segment_id = segments.id
          )
        """,
        (transcript_id, *sorted(valid_indexes)),
    ).fetchall()
    stale_ids = [row["id"] for row in rows]
    if not stale_ids:
        return 0
    stale_placeholders = ", ".join("?" for _ in stale_ids)
    conn.execute(
        f"""
        DELETE FROM jobs
        WHERE job_type = 'label_segment'
          AND status IN ('pending', 'failed')
          AND target_id IN ({stale_placeholders})
        """,
        stale_ids,
    )
    conn.execute(f"DELETE FROM content_spans WHERE legacy_segment_id IN ({stale_placeholders})", stale_ids)
    conn.execute(f"DELETE FROM segments WHERE id IN ({stale_placeholders})", stale_ids)
    return len(stale_ids)


def transcript_candidates(
    conn,
    *,
    lane: str,
    limit: int,
    claim: bool = False,
    worker_id: str = "transcript-discovery",
    lease_minutes: int = 60,
    max_claimed: int | None = None,
    per_source_limit: int | None = None,
) -> list[dict[str, Any]]:
    job_ids: list[int] | None = None
    if claim:
        now = now_iso()
        leased_until = (dt.datetime.fromisoformat(now) + dt.timedelta(minutes=lease_minutes)).isoformat()
        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")
        effective_limit = limit
        if max_claimed is not None:
            active_claimed = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM jobs
                    WHERE lane = ?
                      AND job_type = 'manual_transcript_required'
                      AND status = 'claimed'
                      AND leased_until >= ?
                    """,
                    (lane, now),
                ).fetchone()["count"]
            )
            remaining = max(max_claimed - active_claimed, 0)
            if remaining <= 0:
                conn.commit()
                return []
            effective_limit = min(effective_limit, remaining)
        if per_source_limit is not None:
            rows = conn.execute(
                """
                UPDATE jobs
                SET status = 'claimed',
                    lease_owner = ?,
                    leased_until = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id IN (
                  SELECT id
                  FROM (
                    SELECT
                      jobs.id,
                      jobs.priority,
                      ROW_NUMBER() OVER (PARTITION BY episodes.source_id ORDER BY jobs.priority ASC, jobs.id ASC) AS source_rank,
                      (
                        SELECT COUNT(*)
                        FROM jobs AS active_jobs
                        JOIN episodes AS active_episodes ON active_episodes.id = active_jobs.target_id
                        WHERE active_jobs.lane = jobs.lane
                          AND active_jobs.job_type = 'manual_transcript_required'
                          AND active_jobs.status = 'claimed'
                          AND active_jobs.leased_until >= ?
                          AND active_episodes.source_id = episodes.source_id
                      ) AS active_claims
                    FROM jobs
                    JOIN episodes ON episodes.id = jobs.target_id
                    WHERE jobs.lane = ?
                      AND jobs.job_type = 'manual_transcript_required'
                      AND jobs.attempts < jobs.max_attempts
                      AND (jobs.status = 'pending' OR (jobs.status = 'claimed' AND jobs.leased_until < ?))
                  )
                  WHERE active_claims < ?
                    AND source_rank <= (? - active_claims)
                  ORDER BY priority ASC, id ASC
                  LIMIT ?
                )
                RETURNING id
                """,
                (worker_id, leased_until, now, now, lane, now, per_source_limit, per_source_limit, effective_limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                UPDATE jobs
                SET status = 'claimed',
                    lease_owner = ?,
                    leased_until = ?,
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id IN (
                  SELECT jobs.id
                  FROM jobs
                  JOIN episodes ON episodes.id = jobs.target_id
                  WHERE jobs.lane = ?
                    AND jobs.job_type = 'manual_transcript_required'
                    AND jobs.attempts < jobs.max_attempts
                    AND (jobs.status = 'pending' OR (jobs.status = 'claimed' AND jobs.leased_until < ?))
                  ORDER BY jobs.priority ASC, jobs.id ASC
                  LIMIT ?
                )
                RETURNING id
                """,
                (worker_id, leased_until, now, lane, now, effective_limit),
            ).fetchall()
        job_ids = [int(row["id"]) for row in rows]
        if not job_ids:
            conn.commit()
            return []
        conn.commit()
    status_filter = "jobs.status = 'pending'"
    limit_clause = ""
    params: list[Any] = [lane]
    if job_ids is not None:
        placeholders = ", ".join("?" for _ in job_ids)
        status_filter = f"jobs.id IN ({placeholders})"
        params.extend(job_ids)
    else:
        limit_clause = "LIMIT ?"
        params.append(limit)
    return [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT
              jobs.id AS job_id,
              jobs.status AS job_status,
              jobs.lease_owner,
              jobs.leased_until,
              episodes.id AS episode_id,
              episodes.title AS episode_title,
              episodes.url AS episode_url,
              episodes.published_at,
              sources.name AS source_name,
              sources.homepage_url,
              sources.transcript_policy,
              transcript_acquisition_status.status AS acquisition_status,
              transcript_acquisition_status.attempts_count AS acquisition_attempts_count,
              transcript_acquisition_status.eligible_for_transcription
            FROM jobs
            JOIN episodes ON episodes.id = jobs.target_id
            JOIN sources ON sources.id = episodes.source_id
            LEFT JOIN transcript_acquisition_status ON transcript_acquisition_status.episode_id = episodes.id
            WHERE jobs.lane = ?
              AND jobs.job_type = 'manual_transcript_required'
              AND {status_filter}
            ORDER BY jobs.priority ASC, jobs.id ASC
            {limit_clause}
            """,
            params,
        ).fetchall()
    ]


def verify_sources(source_list: str | Path) -> dict[str, Any]:
    sources = load_source_list(source_list)
    results = []
    for source in sources:
        rss_url = source.get("rss_url")
        result: dict[str, Any] = {"name": source.get("name"), "rss_url": rss_url, "ok": False, "episodes": 0, "transcript_links": 0}
        if not rss_url:
            result["error"] = "missing rss_url"
            results.append(result)
            continue
        try:
            feed_text, _ = fetch_url(str(rss_url))
            episodes = parse_feed(feed_text, source_id=source.get("id") or slugify(source["name"]))
            result["ok"] = True
            result["episodes"] = len(episodes)
            result["transcript_links"] = sum(1 for episode in episodes if episode.get("transcript_url"))
        except Exception as exc:
            result["error"] = _short_error(exc)
        results.append(result)
    return {
        "sources": len(results),
        "ok": sum(1 for item in results if item.get("ok")),
        "failed": sum(1 for item in results if not item.get("ok")),
        "transcript_link_sources": sum(1 for item in results if item.get("transcript_links", 0) > 0),
        "results": results,
    }


def release_transcript_candidate_claims(
    conn,
    *,
    lane: str,
    worker_id: str | None = None,
    expired_only: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    """Release unresolved manual transcript discovery claims back to pending."""
    ts = now_iso()
    filters = [
        "jobs.lane = ?",
        "jobs.job_type = 'manual_transcript_required'",
        "jobs.status = 'claimed'",
        "episodes.transcript_url IS NULL",
    ]
    params: list[Any] = [lane]
    if worker_id:
        filters.append("jobs.lease_owner = ?")
        params.append(worker_id)
    if expired_only:
        filters.append("jobs.leased_until IS NOT NULL")
        filters.append("jobs.leased_until < ?")
        params.append(ts)
    rows = conn.execute(
        f"""
        SELECT jobs.id
        FROM jobs
        JOIN episodes ON episodes.id = jobs.target_id
        WHERE {" AND ".join(filters)}
        ORDER BY jobs.updated_at ASC, jobs.id ASC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    job_ids = [int(row["id"]) for row in rows]
    if not job_ids:
        conn.commit()
        return {"ok": True, "released": 0, "lane": lane, "worker_id": worker_id, "expired_only": expired_only}
    placeholders = ", ".join("?" for _ in job_ids)
    conn.execute(
        f"""
        UPDATE jobs
        SET status = 'pending',
            lease_owner = NULL,
            leased_until = NULL,
            attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
            error = ?,
            updated_at = ?
        WHERE id IN ({placeholders})
        """,
        ("Released unresolved transcript discovery claim for retry.", ts, *job_ids),
    )
    conn.commit()
    return {"ok": True, "released": len(job_ids), "lane": lane, "worker_id": worker_id, "expired_only": expired_only}


def attach_transcript(
    conn,
    *,
    episode_id: str,
    transcript_url: str,
    transcript_type: str | None,
    source_kind: str,
    lane: str,
    label_pack: str,
) -> dict[str, Any]:
    episode = conn.execute("SELECT id FROM episodes WHERE id = ?", (episode_id,)).fetchone()
    if not episode:
        raise ValueError(f"Episode not found: {episode_id}")
    ts = now_iso()
    conn.execute(
        """
        UPDATE episodes
        SET verified_transcript_url = ?,
            verified_transcript_type = ?,
            verified_transcript_source_kind = ?,
            transcript_url = ?,
            transcript_type = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (transcript_url, transcript_type, source_kind, transcript_url, transcript_type, ts, episode_id),
    )
    record_transcript_source_found(
        conn,
        episode_id=episode_id,
        source_kind=source_kind,
        result_url=transcript_url,
    )
    complete_diagnostic_jobs(conn, lane=lane, job_type="manual_transcript_required", target_id=episode_id)
    job_id = db.enqueue_job(
        conn,
        lane=lane,
        job_type="fetch_transcript",
        target_id=episode_id,
        payload={"label_pack": label_pack, "source_kind": source_kind},
        priority=30,
    )
    conn.commit()
    return {"episode_id": episode_id, "fetch_job_id": job_id, "source_kind": source_kind}


def _text(item: ET.Element, tag: str) -> str | None:
    found = item.find(tag)
    if found is not None and found.text:
        return found.text
    return None


def _attr(item: ET.Element, tag: str, attr: str) -> str | None:
    found = item.find(tag)
    if found is not None:
        return found.get(attr)
    return None


def _transcript_link(item: ET.Element) -> tuple[str | None, str | None]:
    for element in item.iter():
        if element.tag.endswith("transcript"):
            url = element.get("url") or element.get("href")
            if url:
                return url, element.get("type")
    return None, None


def _duration_seconds(value: str | None) -> int | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return int(value)
    parts = value.split(":")
    try:
        nums = [int(part) for part in parts]
    except ValueError:
        return None
    total = 0
    for num in nums:
        total = total * 60 + num
    return total
