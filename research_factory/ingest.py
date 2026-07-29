from __future__ import annotations

import datetime as dt
import gzip
import html
import json
import mimetypes
import re
import sqlite3
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse
from zoneinfo import ZoneInfo

from . import db
from .paths import corpus_dir
from .sources import load_source_list
from .text import TranscriptQuality, analyze_transcript_quality, segment_text, transcript_to_text
from .transcript_strategies import load_transcript_strategies
from .util import dumps_json, loads_json, now_iso, parse_date, parse_datetime, read_text, sha256_text, slugify, stable_id, write_text_atomic


PODCAST_NS = "{https://podcastindex.org/namespace/1.0}"
ITUNES_NS = "{http://www.itunes.com/dtds/podcast-1.0.dtd}"
ATOM_NS = "{http://www.w3.org/2005/Atom}"
CONTENT_NS = "{http://purl.org/rss/1.0/modules/content/}"

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
TRANSCRIPT_ATTEMPT_DEFAULT_COOLDOWN_SECONDS = 24 * 60 * 60
TRANSCRIPT_ATTEMPT_TERMINAL_STATUSES = {
    TRANSCRIPTION_ELIGIBLE_STATUS,
    "rss_transcript_found",
    "official_page_found",
    "youtube_caption_found",
    "transcript_source_found",
    "voyager_transcription_completed",
}
TRANSCRIPT_DIRECT_EXTENSIONS = {".vtt", ".srt", ".json", ".txt", ".md"}
TRANSCRIPT_DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx"}
TRANSCRIPT_LINK_RE = re.compile(r"""(?i)<a\b[^>]*\bhref=["']([^"']+)["'][^>]*>(.*?)</a>""", re.S)
RAW_URL_RE = re.compile(r"""https?://[^\s"'<>]+""", re.I)
SIMPLECAST_TRANSCRIPT_RE = re.compile(r"""https?://[^"'\s<>]+\.simplecast\.com/episodes/[^"'\s<>]+/transcript\b""", re.I)
SUBSTACK_RSS_POST_TRANSCRIPT_SOURCE_IDS = {"latent-space", "the-gradient", "the-pragmatic-engineer"}
SUBSTACK_TRANSCRIPT_HOSTS = {
    "www.latent.space",
    "latent.space",
    "thegradientpub.substack.com",
    "newsletter.pragmaticengineer.com",
}
DIRECT_OFFICIAL_PAGE_TRANSCRIPT_HOSTS_BY_SOURCE_ID = {
    "corecursive": {"corecursive.com", "www.corecursive.com"},
    "the-cognitive-revolution": {"www.cognitiverevolution.ai", "cognitiverevolution.ai"},
}
VERGE_OFFICIAL_TRANSCRIPT_FEEDS = {
    "decoder-with-nilay-patel": "https://www.theverge.com/rss/decoder-podcast-with-nilay-patel/index.xml",
    "the-vergecast": "https://www.theverge.com/rss/the-vergecast/index.xml",
}
VERGE_TRANSCRIPT_HOSTS = {"www.theverge.com", "theverge.com"}
VERGE_MIN_TRANSCRIPT_WORDS = 1000
GCP_OFFICIAL_ARCHIVE_URL = "https://www.gcppodcast.com/post/"
GCP_OFFICIAL_SOURCE_ID = "google-cloud-platform-podcast"
GCP_MIN_TRANSCRIPT_WORDS = 100


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
        raw = response.read()
        if raw.startswith(b"\x1f\x8b"):
            raw = gzip.decompress(raw)
        body = raw.decode(charset, errors="replace")
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
        if _is_simplecast_transcript_url(url):
            return _fetch_simplecast_transcription(url)
        body, content_type = fetch_url(url)
        simplecast_url = _extract_simplecast_transcript_url(body)
        if simplecast_url:
            return _fetch_simplecast_transcription(simplecast_url)
        return body, content_type
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except ImportError as exc:
        raise RuntimeError("youtube-transcript-api is required for youtube_captions sources") from exc
    video_id = _youtube_video_id(url)
    fetched = YouTubeTranscriptApi().fetch(video_id, languages=("en",))
    text = "\n".join(snippet.text.strip() for snippet in fetched if snippet.text.strip())
    return text, transcript_type or "text/plain"


def _is_simplecast_transcript_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc.lower().endswith(".simplecast.com") and parsed.path.rstrip("/").endswith("/transcript")


def _extract_simplecast_transcript_url(body: str) -> str | None:
    match = SIMPLECAST_TRANSCRIPT_RE.search(html.unescape(body))
    return match.group(0) if match else None


def _simplecast_episode_url(transcript_url: str) -> str:
    parsed = urlparse(transcript_url)
    path = parsed.path.rstrip("/")
    if path.endswith("/transcript"):
        path = path[: -len("/transcript")]
    return parsed._replace(path=path, query="", fragment="").geturl()


def _fetch_simplecast_transcription(transcript_url: str) -> tuple[str, str | None]:
    episode_url = _simplecast_episode_url(transcript_url)
    payload = json.dumps({"url": episode_url}).encode("utf-8")
    req = urllib.request.Request(
        "https://api.simplecast.com/episodes/search",
        data=payload,
        headers={
            "User-Agent": "podcast-intelligence-factory/0.1",
            "Content-Type": "application/json",
            "Origin": f"{urlparse(episode_url).scheme}://{urlparse(episode_url).netloc}",
            "Referer": episode_url,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8", errors="replace"))
    transcription = data.get("transcription")
    if not isinstance(transcription, str) or len(transcription.split()) < 20:
        raise ValueError("Simplecast transcript missing or too short")
    text = transcript_to_text(transcription, "text/html", None)
    if len(text.split()) < 20:
        raise ValueError("Simplecast transcript missing or too short")
    return text, "text/plain"


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
    idempotency_key: str | None = None,
    cooldown_seconds: int | None = None,
) -> dict[str, Any]:
    episode = conn.execute("SELECT id, source_id FROM episodes WHERE id = ?", (episode_id,)).fetchone()
    if not episode:
        raise ValueError(f"Episode not found: {episode_id}")
    ts = now_iso()
    terminal = status in TRANSCRIPT_ATTEMPT_TERMINAL_STATUSES or status.endswith("_completed")
    if cooldown_seconds is None:
        cooldown_seconds = 0 if terminal or status == "browser_discovery_pending" else TRANSCRIPT_ATTEMPT_DEFAULT_COOLDOWN_SECONDS
    if cooldown_seconds < 0:
        raise ValueError("cooldown_seconds must be non-negative")
    bucket = "terminal" if terminal else ts[:10]
    resolved_idempotency_key = idempotency_key or stable_id(
        episode_id,
        method,
        status,
        result_url or "",
        result_source_kind or "",
        error_class or "",
        bucket,
        prefix="tak_",
    )
    existing = conn.execute(
        """
        SELECT id, next_eligible_at
        FROM transcript_acquisition_attempts
        WHERE idempotency_key = ?
        """,
        (resolved_idempotency_key,),
    ).fetchone()
    if existing:
        current = conn.execute(
            "SELECT status, eligible_for_transcription, attempts_count, next_eligible_at FROM transcript_acquisition_status WHERE episode_id = ?",
            (episode_id,),
        ).fetchone()
        return {
            "attempt_id": existing["id"],
            "episode_id": episode_id,
            "method": method,
            "status": current["status"] if current else status,
            "eligible_for_transcription": bool(current["eligible_for_transcription"]) if current else terminal,
            "attempts_count": int(current["attempts_count"] or 0) if current else 1,
            "next_eligible_at": current["next_eligible_at"] if current else existing["next_eligible_at"],
            "duplicate": True,
        }
    next_eligible_at = None
    if cooldown_seconds:
        next_eligible_at = (
            dt.datetime.fromisoformat(ts).astimezone(dt.timezone.utc) + dt.timedelta(seconds=cooldown_seconds)
        ).replace(microsecond=0).isoformat()
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
            official_public, policy_allowed, error_class, notes, worker_id,
            idempotency_key, next_eligible_at, metadata_json, created_at
          )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            resolved_idempotency_key,
            next_eligible_at,
            dumps_json(metadata or {}),
            ts,
        ),
    )
    conn.execute(
        """
        INSERT INTO transcript_acquisition_status
          (episode_id, source_id, status, last_method, attempts_count, eligible_for_transcription, next_eligible_at, updated_at)
        VALUES (?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(episode_id) DO UPDATE SET
          status = excluded.status,
          source_id = excluded.source_id,
          last_method = excluded.last_method,
          attempts_count = transcript_acquisition_status.attempts_count + 1,
          eligible_for_transcription = excluded.eligible_for_transcription,
          next_eligible_at = excluded.next_eligible_at,
          updated_at = excluded.updated_at
        """,
        (episode_id, episode["source_id"], status, method, eligible, next_eligible_at, ts),
    )
    return {
        "attempt_id": attempt_id,
        "episode_id": episode_id,
        "method": method,
        "status": status,
        "eligible_for_transcription": bool(eligible),
        "attempts_count": attempt_index,
        "next_eligible_at": next_eligible_at,
        "duplicate": False,
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


def source_kind_for_transcript_url(url: str, transcript_type: str | None = None) -> str:
    lowered_url = url.lower()
    lowered_type = (transcript_type or "").lower()
    host = urlparse(url).netloc.lower()
    path = urlparse(url).path.lower()
    if "youtube.com" in host or host.endswith("youtu.be"):
        return "youtube_captions"
    if any(path.endswith(ext) for ext in TRANSCRIPT_DIRECT_EXTENSIONS):
        return "creator_provided_rss_transcript"
    if any(token in lowered_type for token in ["vtt", "subrip", "json", "plain"]):
        return "creator_provided_rss_transcript"
    if "format=webvtt" in lowered_url or "format=subrip" in lowered_url:
        return "creator_provided_rss_transcript"
    return "official_show_transcript"


def enqueue_transcript_backlog(
    conn,
    *,
    lane: str,
    label_pack: str,
    limit: int,
    source_filter: list[str] | None = None,
    include_youtube_captions: bool = False,
    include_document_transcripts: bool = False,
    include_quarantined_retry: bool = False,
    priority: int = 35,
    dry_run: bool = False,
) -> dict[str, Any]:
    if limit < 1:
        raise ValueError("--limit must be at least 1")
    filters = [
        "COALESCE(episodes.verified_transcript_url, episodes.feed_transcript_url, episodes.transcript_url) IS NOT NULL",
        """
        NOT EXISTS (
          SELECT 1
          FROM transcripts
          WHERE transcripts.episode_id = episodes.id
            AND transcripts.status = 'ready'
        )
        """,
        """
        NOT EXISTS (
          SELECT 1
          FROM jobs
          WHERE jobs.lane = ?
            AND jobs.job_type = 'fetch_transcript'
            AND jobs.target_id = episodes.id
            AND jobs.status IN ('pending', 'claimed')
        )
        """,
        """
        NOT EXISTS (
          SELECT 1
          FROM jobs AS failed_jobs
          WHERE failed_jobs.lane = ?
            AND failed_jobs.job_type = 'fetch_transcript'
            AND failed_jobs.target_id = episodes.id
            AND failed_jobs.status = 'failed'
            AND failed_jobs.attempts >= failed_jobs.max_attempts
            AND (
              failed_jobs.error LIKE 'HTTP Error 404:%'
              OR failed_jobs.error LIKE 'HTTP Error 410:%'
              OR failed_jobs.error LIKE 'HTTP Error 403:%'
              OR failed_jobs.error LIKE 'Simplecast transcript missing or too short%'
              {parser_retry_blocked_errors}
            )
        )
        """,
    ]
    parser_retry_blocked_errors = (
        ""
        if include_quarantined_retry
        else """
              OR failed_jobs.error LIKE 'Transcript too short after parsing:%'
              OR failed_jobs.error LIKE '%official_page_boilerplate_shell%'
        """
    )
    filters[-1] = filters[-1].format(parser_retry_blocked_errors=parser_retry_blocked_errors)
    params: list[Any] = [lane, lane]
    if not include_quarantined_retry:
        filters.append(
            """
            NOT EXISTS (
              SELECT 1
              FROM transcripts AS quarantined_transcripts
              WHERE quarantined_transcripts.episode_id = episodes.id
                AND quarantined_transcripts.status = 'quarantined'
            )
            """
        )
    if source_filter:
        wanted = {_source_filter_key(item) for item in source_filter}
        placeholders = ", ".join("?" for _ in wanted)
        filters.append(f"(LOWER(sources.id) IN ({placeholders}) OR LOWER(sources.name) IN ({placeholders}))")
        params.extend(wanted)
        params.extend(wanted)
    rows = conn.execute(
        f"""
        SELECT
          episodes.id AS episode_id,
          episodes.title AS episode_title,
          episodes.transcript_url,
          episodes.transcript_type,
          episodes.feed_transcript_url,
          episodes.feed_transcript_type,
          episodes.verified_transcript_url,
          episodes.verified_transcript_type,
          episodes.verified_transcript_source_kind,
          sources.id AS source_id,
          sources.name AS source_name,
          COUNT(transcripts.id) AS transcript_attempts
        FROM episodes
        JOIN sources ON sources.id = episodes.source_id
        LEFT JOIN transcripts ON transcripts.episode_id = episodes.id
        WHERE {" AND ".join(filters)}
        GROUP BY episodes.id
        ORDER BY
          CASE WHEN COUNT(transcripts.id) = 0 THEN 0 ELSE 1 END,
          episodes.published_at DESC,
          episodes.id ASC
        LIMIT ?
        """,
        (*params, limit * 3),
    ).fetchall()
    strategy_policy = _safe_transcript_backlog_strategy_policy()
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    by_source_kind: dict[str, int] = {}
    for row in rows:
        if len(selected) >= limit:
            break
        if _source_blocked_by_strategy_policy(row["source_id"], row["source_name"], strategy_policy):
            skipped.append({"episode_id": row["episode_id"], "source_name": row["source_name"], "reason": "strategy_not_ready"})
            continue
        transcript_url = row["verified_transcript_url"] or row["feed_transcript_url"] or row["transcript_url"]
        transcript_type = row["verified_transcript_type"] or row["feed_transcript_type"] or row["transcript_type"]
        source_kind = row["verified_transcript_source_kind"] or source_kind_for_transcript_url(transcript_url, transcript_type)
        resolved = _resolve_backlog_transcript_url(
            source_name=row["source_name"],
            transcript_url=transcript_url,
            transcript_type=transcript_type,
            source_kind=source_kind,
        )
        if not resolved["ok"]:
            skipped.append({"episode_id": row["episode_id"], "source_name": row["source_name"], "reason": resolved["reason"]})
            continue
        transcript_url = resolved["transcript_url"]
        transcript_type = resolved["transcript_type"]
        source_kind = resolved["source_kind"]
        path = urlparse(transcript_url).path.lower()
        if source_kind == "youtube_captions" and not include_youtube_captions:
            skipped.append({"episode_id": row["episode_id"], "source_name": row["source_name"], "reason": "youtube_caption_lane_disabled"})
            continue
        if any(path.endswith(ext) for ext in TRANSCRIPT_DOCUMENT_EXTENSIONS) and not include_document_transcripts:
            skipped.append({"episode_id": row["episode_id"], "source_name": row["source_name"], "reason": "document_transcript_lane_disabled"})
            continue
        job_id = None
        if not dry_run:
            conn.execute(
                """
                UPDATE episodes
                SET transcript_url = ?,
                    transcript_type = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (transcript_url, transcript_type, now_iso(), row["episode_id"]),
            )
            job_payload = {"label_pack": label_pack, "source_kind": source_kind, "strategy_backlog": True}
            existing_job = conn.execute(
                """
                SELECT id, dedupe_key
                FROM jobs
                WHERE lane = ?
                  AND job_type = 'fetch_transcript'
                  AND target_id = ?
                  AND status IN ('failed', 'completed')
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (lane, row["episode_id"]),
            ).fetchone()
            if existing_job:
                job_id = int(existing_job["id"])
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        payload_json = ?,
                        priority = ?,
                        attempts = 0,
                        lease_owner = NULL,
                        leased_until = NULL,
                        error = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (dumps_json(job_payload), priority, now_iso(), job_id),
                )
            else:
                job_id = db.enqueue_job(
                    conn,
                    lane=lane,
                    job_type="fetch_transcript",
                    target_id=row["episode_id"],
                    payload=job_payload,
                    priority=priority,
                )
        if dry_run or job_id:
            selected.append(
                {
                    "episode_id": row["episode_id"],
                    "source_name": row["source_name"],
                    "source_kind": source_kind,
                    "job_id": job_id,
                    "transcript_attempts": int(row["transcript_attempts"] or 0),
                }
            )
            by_source_kind[source_kind] = by_source_kind.get(source_kind, 0) + 1
    if not dry_run:
        conn.commit()
    return {
        "ok": True,
        "dry_run": dry_run,
        "lane": lane,
        "label_pack": label_pack,
        "matched": len(rows),
        "enqueued": 0 if dry_run else len(selected),
        "would_enqueue": len(selected) if dry_run else 0,
        "by_source_kind": by_source_kind,
        "include_youtube_captions": include_youtube_captions,
        "include_document_transcripts": include_document_transcripts,
        "include_quarantined_retry": include_quarantined_retry,
        "skipped": skipped[:100],
        "jobs": selected[:100],
        "privacy": "sanitized_operational_report_no_raw_transcripts",
    }


def _resolve_backlog_transcript_url(
    *,
    source_name: str,
    transcript_url: str,
    transcript_type: str | None,
    source_kind: str,
) -> dict[str, Any]:
    parsed = urlparse(transcript_url)
    if source_name == "The Stack Overflow Podcast" and parsed.netloc.lower() == "stackoverflow.blog":
        try:
            body, _ = fetch_url(transcript_url)
        except Exception:
            return {"ok": False, "reason": "official_transcript_link_fetch_failed"}
        simplecast_url = _extract_simplecast_transcript_url(body)
        if not simplecast_url:
            return {"ok": False, "reason": "official_transcript_link_not_found"}
        return {
            "ok": True,
            "transcript_url": simplecast_url,
            "transcript_type": "text/html",
            "source_kind": "official_show_transcript",
        }
    return {
        "ok": True,
        "transcript_url": transcript_url,
        "transcript_type": transcript_type,
        "source_kind": source_kind,
    }


def _safe_transcript_backlog_strategy_policy() -> dict[str, set[str]]:
    ready: set[str] = set()
    blocked: set[str] = set()
    try:
        strategies = load_transcript_strategies()
    except Exception:
        return {"ready": ready, "blocked": blocked}
    for strategy in strategies:
        sources = strategy.get("sources") or []
        keys = {_source_filter_key(str(source)) for source in sources}
        if strategy.get("status") == "ready" and strategy.get("lane") == "automatic_fetch":
            ready.update(keys)
        else:
            blocked.update(keys)
    return {"ready": ready, "blocked": blocked}


def _source_blocked_by_strategy_policy(source_id: str, source_name: str, policy: dict[str, set[str]]) -> bool:
    keys = {_source_filter_key(source_id), _source_filter_key(source_name)}
    if keys & policy["ready"]:
        return False
    return bool(keys & policy["blocked"])


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


def enqueue_sources(
    conn,
    source_list: str | Path,
    *,
    lane: str,
    since: str | None,
    label_pack: str,
    until: str | None = None,
    source_filter: list[str] | None = None,
    max_items: int | None = None,
    per_source_limit: int | None = None,
    oldest_first: bool = False,
    dry_run: bool = False,
    enqueue_transcripts: bool = True,
    fetch_concurrency: int = 1,
) -> dict[str, Any]:
    sources = _filter_sources(load_source_list(source_list), source_filter)
    since_dt = parse_date(since)
    until_dt = parse_date(until)
    until_exclusive = _until_exclusive(until, until_dt)
    stats: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "enqueue_transcripts": enqueue_transcripts,
        "sources": 0,
        "sources_with_rss": 0,
        "feed_items_seen": 0,
        "episodes": 0,
        "episodes_inserted": 0,
        "episodes_existing": 0,
        "transcript_jobs": 0,
        "missing_transcripts": 0,
        "source_errors": 0,
        "skipped_before_since": 0,
        "skipped_after_until": 0,
        "skipped_after_max_items": 0,
        "skipped_after_per_source_limit": 0,
        "source_stats": [],
    }
    if max_items is not None and max_items < 1:
        raise ValueError("--max-items must be at least 1")
    if per_source_limit is not None and per_source_limit < 1:
        raise ValueError("--per-source-limit must be at least 1")

    source_inputs: list[tuple[dict[str, Any], str]] = []
    for source in sources:
        source_id = _source_id(source)
        if not dry_run:
            source_id = upsert_source(conn, source)
            conn.commit()
        stats["sources"] += 1
        if source.get("rss_url"):
            stats["sources_with_rss"] += 1
            source_inputs.append((source, source_id))
        else:
            stats["source_stats"].append({"source_id": source_id, "source": source["name"], "feed_items_seen": 0, "selected": 0, "error": None})

    selected_total = 0
    for result in _fetch_feeds(source_inputs, concurrency=fetch_concurrency):
        source = result["source"]
        source_id = result["source_id"]
        source_name = source["name"]
        source_stat: dict[str, Any] = {
            "source_id": source_id,
            "source": source_name,
            "feed_items_seen": 0,
            "selected": 0,
            "inserted": 0,
            "existing": 0,
            "transcript_jobs": 0,
            "missing_transcripts": 0,
            "skipped_before_since": 0,
            "skipped_after_until": 0,
            "skipped_after_per_source_limit": 0,
            "skipped_after_max_items": 0,
            "error": None,
        }
        if result.get("error"):
            source_stat["error"] = result["error"]
            stats["source_errors"] += 1
            if not dry_run:
                db.enqueue_job(
                    conn,
                    lane=lane,
                    job_type="source_fetch_failed",
                    target_id=source_id,
                    payload={"rss_url": str(source.get("rss_url")), "error": str(result["error"])},
                    priority=750,
                    max_attempts=1,
                )
                conn.commit()
            stats["source_stats"].append(source_stat)
            continue

        if not dry_run:
            complete_diagnostic_jobs(conn, lane=lane, job_type="source_fetch_failed", target_id=source_id)
        episodes = list(result["episodes"])
        if oldest_first:
            episodes.sort(key=_episode_sort_key)
        source_selected = 0
        for episode in episodes:
            source_stat["feed_items_seen"] += 1
            stats["feed_items_seen"] += 1
            published = parse_datetime(episode.get("published_at"))
            if since_dt and published and published < since_dt:
                source_stat["skipped_before_since"] += 1
                stats["skipped_before_since"] += 1
                continue
            if until_exclusive and published and published >= until_exclusive:
                source_stat["skipped_after_until"] += 1
                stats["skipped_after_until"] += 1
                continue
            if per_source_limit is not None and source_selected >= per_source_limit:
                source_stat["skipped_after_per_source_limit"] += 1
                stats["skipped_after_per_source_limit"] += 1
                continue
            if max_items is not None and selected_total >= max_items:
                source_stat["skipped_after_max_items"] += 1
                stats["skipped_after_max_items"] += 1
                continue

            existing = conn.execute(
                "SELECT id FROM episodes WHERE source_id = ? AND guid = ?",
                (episode["source_id"], episode["guid"]),
            ).fetchone()
            episode_id = existing["id"] if existing else episode["id"]
            if not dry_run:
                episode_id = upsert_episode(conn, episode)
            source_selected += 1
            selected_total += 1
            source_stat["selected"] += 1
            stats["episodes"] += 1
            if existing:
                source_stat["existing"] += 1
                stats["episodes_existing"] += 1
            else:
                source_stat["inserted"] += 1
                stats["episodes_inserted"] += 1

            if enqueue_transcripts and not dry_run:
                transcript_result = _enqueue_transcript_work(conn, lane=lane, label_pack=label_pack, episode_id=episode_id, episode=episode)
                source_stat["transcript_jobs"] += transcript_result["transcript_jobs"]
                source_stat["missing_transcripts"] += transcript_result["missing_transcripts"]
                stats["transcript_jobs"] += transcript_result["transcript_jobs"]
                stats["missing_transcripts"] += transcript_result["missing_transcripts"]
        if not dry_run:
            conn.commit()
        stats["source_stats"].append(source_stat)
    if not dry_run:
        conn.commit()
    return stats


def _enqueue_transcript_work(conn, *, lane: str, label_pack: str, episode_id: str, episode: dict[str, Any]) -> dict[str, int]:
    ready = conn.execute(
        """
        SELECT 1
        FROM transcripts
        WHERE episode_id = ?
          AND status = 'ready'
        LIMIT 1
        """,
        (episode_id,),
    ).fetchone()
    if ready:
        return {"transcript_jobs": 0, "missing_transcripts": 0}
    existing_job = conn.execute(
        """
        SELECT 1
        FROM jobs
        WHERE lane = ?
          AND job_type = 'fetch_transcript'
          AND target_id = ?
        LIMIT 1
        """,
        (lane, episode_id),
    ).fetchone()
    if existing_job:
        return {"transcript_jobs": 0, "missing_transcripts": 0}
    if episode.get("transcript_url"):
        source_kind = episode.get("verified_transcript_source_kind") or source_kind_for_transcript_url(str(episode["transcript_url"]), episode.get("transcript_type"))
        record_transcript_source_found(
            conn,
            episode_id=episode_id,
            source_kind=source_kind,
            result_url=str(episode["transcript_url"]),
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
        return {"transcript_jobs": 1 if job_id else 0, "missing_transcripts": 0}
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
    return {"transcript_jobs": 0, "missing_transcripts": 1}


def _fetch_feeds(source_inputs: list[tuple[dict[str, Any], str]], *, concurrency: int) -> list[dict[str, Any]]:
    if concurrency < 1:
        raise ValueError("--fetch-concurrency must be at least 1")
    if concurrency == 1 or len(source_inputs) <= 1:
        return [_fetch_one_feed(source, source_id) for source, source_id in source_inputs]
    results: dict[int, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(concurrency, len(source_inputs))) as executor:
        futures = {
            executor.submit(_fetch_one_feed, source, source_id): index
            for index, (source, source_id) in enumerate(source_inputs)
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return [results[index] for index in range(len(source_inputs))]


def _fetch_one_feed(source: dict[str, Any], source_id: str) -> dict[str, Any]:
    try:
        feed_text, _ = fetch_url(str(source["rss_url"]))
        return {"source": source, "source_id": source_id, "episodes": parse_feed(feed_text, source_id=source_id)}
    except Exception as exc:
        return {"source": source, "source_id": source_id, "episodes": [], "error": _short_error(exc)}


def sync_verge_official_transcripts(
    conn,
    *,
    lane: str,
    label_pack: str,
    limit: int = 50,
    source_filter: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    wanted = {_source_filter_key(item) for item in source_filter or []}
    selected_sources = [
        (source_id, feed_url)
        for source_id, feed_url in VERGE_OFFICIAL_TRANSCRIPT_FEEDS.items()
        if not wanted or _source_filter_key(source_id) in wanted
    ]
    stats: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "lane": lane,
        "label_pack": label_pack,
        "feeds": len(selected_sources),
        "feed_items_seen": 0,
        "matched": 0,
        "attached": 0,
        "fetch_jobs": 0,
        "skipped": [],
        "jobs": [],
        "privacy": "sanitized_operational_report_no_raw_transcripts",
    }
    for source_id, feed_url in selected_sources:
        if stats["attached"] >= limit:
            break
        try:
            feed_text, _ = fetch_url(feed_url)
            feed_episodes = parse_feed(feed_text, source_id=source_id)
        except Exception as exc:
            stats["skipped"].append({"source_id": source_id, "reason": "official_feed_fetch_failed", "error": _short_error(exc)})
            continue
        for feed_episode in feed_episodes:
            if stats["attached"] >= limit:
                break
            stats["feed_items_seen"] += 1
            transcript_url = feed_episode.get("verified_transcript_url") or feed_episode.get("transcript_url")
            if not transcript_url:
                stats["skipped"].append({"source_id": source_id, "reason": "no_official_article_url"})
                continue
            match = _match_existing_episode_for_official_page(conn, source_id=source_id, official_episode=feed_episode)
            if not match:
                stats["skipped"].append({"source_id": source_id, "reason": "no_existing_episode_match"})
                continue
            stats["matched"] += 1
            if _episode_has_ready_transcript(conn, match["id"]):
                stats["skipped"].append({"episode_id": match["id"], "source_id": source_id, "reason": "already_ready"})
                continue
            try:
                body, content_type = fetch_url(str(transcript_url))
                parsed = transcript_to_text(body, content_type or "text/html", str(transcript_url))
            except Exception as exc:
                stats["skipped"].append({"episode_id": match["id"], "source_id": source_id, "reason": "official_page_fetch_failed", "error": _short_error(exc)})
                continue
            if len(parsed.split()) < VERGE_MIN_TRANSCRIPT_WORDS:
                stats["skipped"].append({"episode_id": match["id"], "source_id": source_id, "reason": "official_article_body_too_short"})
                continue
            job_id = None
            if not dry_run:
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
                    (transcript_url, "text/html", "official_show_transcript", transcript_url, "text/html", ts, match["id"]),
                )
                record_transcript_source_found(
                    conn,
                    episode_id=match["id"],
                    source_kind="official_show_transcript",
                    result_url=str(transcript_url),
                )
                complete_diagnostic_jobs(conn, lane=lane, job_type="manual_transcript_required", target_id=match["id"])
                job_payload = {"label_pack": label_pack, "source_kind": "official_show_transcript", "strategy_backlog": True}
                existing_job = conn.execute(
                    """
                    SELECT id
                    FROM jobs
                    WHERE lane = ?
                      AND job_type = 'fetch_transcript'
                      AND target_id = ?
                      AND status IN ('pending', 'claimed', 'failed', 'completed')
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1
                    """,
                    (lane, match["id"]),
                ).fetchone()
                if existing_job:
                    job_id = int(existing_job["id"])
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = 'pending',
                            payload_json = ?,
                            priority = 32,
                            attempts = 0,
                            lease_owner = NULL,
                            leased_until = NULL,
                            error = NULL,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (dumps_json(job_payload), ts, job_id),
                    )
                else:
                    job_id = db.enqueue_job(
                        conn,
                        lane=lane,
                        job_type="fetch_transcript",
                        target_id=match["id"],
                        payload=job_payload,
                        priority=32,
                    )
                conn.commit()
            stats["attached"] += 1
            if job_id:
                stats["fetch_jobs"] += 1
            stats["jobs"].append({"episode_id": match["id"], "source_id": source_id, "job_id": job_id, "word_count": len(parsed.split())})
    if not dry_run:
        conn.commit()
    stats["skipped"] = stats["skipped"][:100]
    stats["jobs"] = stats["jobs"][:100]
    return stats


def sync_gcp_official_transcripts(
    conn,
    *,
    lane: str,
    label_pack: str,
    limit: int = 100,
    max_archive_pages: int = 40,
    dry_run: bool = False,
) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "lane": lane,
        "label_pack": label_pack,
        "archive_pages_checked": 0,
        "official_pages_seen": 0,
        "matched": 0,
        "attached": 0,
        "fetch_jobs": 0,
        "skipped": [],
        "jobs": [],
        "privacy": "sanitized_operational_report_no_raw_transcripts",
    }
    official_urls = _gcp_archive_episode_urls(max_archive_pages=max_archive_pages, stats=stats)
    for transcript_url in official_urls:
        if stats["attached"] >= limit:
            break
        stats["official_pages_seen"] += 1
        try:
            body, content_type = fetch_url(transcript_url)
            parsed = transcript_to_text(body, content_type or "text/html", transcript_url)
        except Exception as exc:
            stats["skipped"].append({"reason": "official_page_fetch_failed", "error": _short_error(exc)})
            continue
        if len(parsed.split()) < GCP_MIN_TRANSCRIPT_WORDS:
            stats["skipped"].append({"reason": "official_transcript_section_too_short"})
            continue
        page_title = _gcp_page_title(body)
        match = _match_existing_episode_by_title(conn, source_id=GCP_OFFICIAL_SOURCE_ID, title=page_title)
        if not match:
            stats["skipped"].append({"reason": "no_existing_episode_match"})
            continue
        stats["matched"] += 1
        if _episode_has_ready_transcript(conn, match["id"]):
            stats["skipped"].append({"episode_id": match["id"], "reason": "already_ready"})
            continue
        job_id = None
        if not dry_run:
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
                (transcript_url, "text/html", "official_show_transcript", transcript_url, "text/html", ts, match["id"]),
            )
            record_transcript_source_found(
                conn,
                episode_id=match["id"],
                source_kind="official_show_transcript",
                result_url=transcript_url,
            )
            complete_diagnostic_jobs(conn, lane=lane, job_type="manual_transcript_required", target_id=match["id"])
            job_payload = {"label_pack": label_pack, "source_kind": "official_show_transcript", "strategy_backlog": True}
            existing_job = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE lane = ?
                  AND job_type = 'fetch_transcript'
                  AND target_id = ?
                  AND status IN ('pending', 'claimed', 'failed', 'completed')
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (lane, match["id"]),
            ).fetchone()
            if existing_job:
                job_id = int(existing_job["id"])
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        payload_json = ?,
                        priority = 32,
                        attempts = 0,
                        lease_owner = NULL,
                        leased_until = NULL,
                        error = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (dumps_json(job_payload), ts, job_id),
                )
            else:
                job_id = db.enqueue_job(
                    conn,
                    lane=lane,
                    job_type="fetch_transcript",
                    target_id=match["id"],
                    payload=job_payload,
                    priority=32,
                )
            conn.commit()
        stats["attached"] += 1
        if job_id:
            stats["fetch_jobs"] += 1
        stats["jobs"].append({"episode_id": match["id"], "job_id": job_id, "word_count": len(parsed.split())})
    if not dry_run:
        conn.commit()
    stats["skipped"] = stats["skipped"][:100]
    stats["jobs"] = stats["jobs"][:100]
    return stats


def _gcp_archive_episode_urls(*, max_archive_pages: int, stats: dict[str, Any]) -> list[str]:
    urls: dict[str, str] = {}
    for page_index in range(1, max_archive_pages + 1):
        archive_url = GCP_OFFICIAL_ARCHIVE_URL if page_index == 1 else f"{GCP_OFFICIAL_ARCHIVE_URL}page/{page_index}/"
        try:
            body, _ = fetch_url(archive_url)
        except Exception:
            break
        stats["archive_pages_checked"] += 1
        found = False
        for raw_link in re.findall(r"href=[\"']([^\"']*/post/episode-[^\"']+/)[\"']", body, flags=re.I):
            found = True
            url = urljoin(archive_url, html.unescape(raw_link))
            urls[url] = url
        if not found and page_index > 1:
            break
    return list(urls.values())


def _gcp_page_title(body: str) -> str:
    match = re.search(r"<h1\b[^>]*class=[\"'][^\"']*\bsr\b[^\"']*[\"'][^>]*>(.*?)</h1>", body, flags=re.I | re.S)
    if match:
        return _clean_html_title(match.group(1))
    title = re.search(r"<title[^>]*>(.*?)</title>", body, flags=re.I | re.S)
    return _clean_html_title(title.group(1)) if title else ""


def _clean_html_title(raw: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(raw))).strip()


def _match_existing_episode_by_title(conn, *, source_id: str, title: str) -> sqlite3.Row | None:
    tokens = _normalized_title_tokens(title)
    if not tokens:
        return None
    candidates = conn.execute(
        """
        SELECT id, title, published_at
        FROM episodes
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchall()
    best: tuple[float, sqlite3.Row] | None = None
    for candidate in candidates:
        candidate_tokens = _normalized_title_tokens(candidate["title"] or "")
        score = _token_jaccard(tokens, candidate_tokens)
        if score < 0.45 and not _title_contains(tokens, candidate_tokens):
            continue
        if best is None or score > best[0]:
            best = (score, candidate)
    return best[1] if best else None


def _episode_has_ready_transcript(conn, episode_id: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM transcripts WHERE episode_id = ? AND status = 'ready' LIMIT 1",
            (episode_id,),
        ).fetchone()
    )


def _match_existing_episode_for_official_page(conn, *, source_id: str, official_episode: dict[str, Any]) -> sqlite3.Row | None:
    published = parse_datetime(official_episode.get("published_at"))
    title = str(official_episode.get("title") or "")
    normalized_title = _normalized_title_tokens(title)
    if not published or not normalized_title:
        return None
    lower = (published - dt.timedelta(days=4)).isoformat()
    upper = (published + dt.timedelta(days=4)).isoformat()
    candidates = conn.execute(
        """
        SELECT id, title, published_at, transcript_url, verified_transcript_url
        FROM episodes
        WHERE source_id = ?
          AND published_at >= ?
          AND published_at <= ?
        """,
        (source_id, lower, upper),
    ).fetchall()
    best: tuple[float, sqlite3.Row] | None = None
    for candidate in candidates:
        candidate_tokens = _normalized_title_tokens(candidate["title"] or "")
        score = _token_jaccard(normalized_title, candidate_tokens)
        if score < 0.34 and not _title_contains(normalized_title, candidate_tokens):
            continue
        if best is None or score > best[0]:
            best = (score, candidate)
    return best[1] if best else None


def _normalized_title_tokens(title: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", html.unescape(title).lower())
        if len(token) > 2 and token not in {"the", "and", "with", "for", "from", "this", "that", "verge", "vergecast", "decoder"}
    }


def _token_jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _title_contains(left: set[str], right: set[str]) -> bool:
    smaller, larger = (left, right) if len(left) <= len(right) else (right, left)
    return bool(smaller) and len(smaller & larger) / len(smaller) >= 0.75


def _source_id(source: dict[str, Any]) -> str:
    return source.get("id") or slugify(source["name"])


def _filter_sources(sources: list[dict[str, Any]], filters: list[str] | None) -> list[dict[str, Any]]:
    if not filters:
        return sources
    wanted = {_source_filter_key(item) for item in filters}
    return [
        source
        for source in sources
        if _source_filter_key(source["name"]) in wanted or _source_filter_key(_source_id(source)) in wanted
    ]


def _source_filter_key(value: str) -> str:
    return slugify(value).casefold()


def _until_exclusive(raw_until: str | None, until_dt: dt.datetime | None) -> dt.datetime | None:
    if until_dt is None:
        return None
    if raw_until and len(raw_until.strip()) == 10:
        return until_dt + dt.timedelta(days=1)
    return until_dt


def _episode_sort_key(episode: dict[str, Any]) -> tuple[str, str]:
    published = parse_datetime(episode.get("published_at"))
    return ((published.isoformat() if published else "9999-12-31T00:00:00+00:00"), str(episode.get("guid") or episode.get("id") or ""))


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
        link = _text(item, "link") or _atom_alternate_link(item) or _attr(item, f"{ATOM_NS}link", "href")
        description = _text(item, "description") or _text(item, f"{ITUNES_NS}summary") or _text(item, "summary")
        feed_body = "\n".join(
            part
            for part in [
                description,
                _text(item, f"{CONTENT_NS}encoded"),
                _text(item, "encoded"),
                _text(item, f"{ATOM_NS}content"),
            ]
            if part
        )
        published_at = _text(item, "pubDate") or _text(item, "published") or _text(item, f"{ATOM_NS}published")
        audio_url = None
        for enclosure in item.findall("enclosure"):
            if (enclosure.get("type") or "").startswith("audio") or enclosure.get("url"):
                audio_url = enclosure.get("url")
                break
        transcript_url, transcript_type = _transcript_link(item, feed_body=feed_body)
        episode_id = stable_id(source_id, guid, prefix="ep_")
        episode = {
            "id": episode_id,
            "source_id": source_id,
            "guid": guid,
            "title": title.strip(),
            "description": (description or "").strip(),
            "url": link,
            "audio_url": audio_url,
            "published_at": parse_datetime(published_at).isoformat() if parse_datetime(published_at) else published_at,
            "duration_seconds": _duration_seconds(_text(item, f"{ITUNES_NS}duration")),
            "feed_transcript_url": transcript_url,
            "feed_transcript_type": transcript_type,
            "verified_transcript_url": None,
            "verified_transcript_type": None,
            "verified_transcript_source_kind": None,
            "transcript_url": transcript_url,
            "transcript_type": transcript_type,
        }
        _apply_source_specific_transcript_route(episode)
        episodes.append(episode)
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
          verified_transcript_url = COALESCE(episodes.verified_transcript_url, excluded.verified_transcript_url),
          verified_transcript_type = COALESCE(episodes.verified_transcript_type, excluded.verified_transcript_type),
          verified_transcript_source_kind = COALESCE(episodes.verified_transcript_source_kind, excluded.verified_transcript_source_kind),
          transcript_url = COALESCE(episodes.verified_transcript_url, excluded.verified_transcript_url, excluded.feed_transcript_url, episodes.transcript_url),
          transcript_type = COALESCE(episodes.verified_transcript_type, excluded.verified_transcript_type, excluded.feed_transcript_type, episodes.transcript_type),
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
            episode.get("feed_transcript_url"),
            episode.get("feed_transcript_type"),
            episode.get("verified_transcript_url"),
            episode.get("verified_transcript_type"),
            episode.get("verified_transcript_source_kind"),
            episode.get("transcript_url"),
            episode.get("transcript_type"),
            ts,
            ts,
        ),
    )
    row = conn.execute("SELECT id FROM episodes WHERE source_id = ? AND guid = ?", (episode["source_id"], episode["guid"])).fetchone()
    return row["id"]


def _apply_source_specific_transcript_route(episode: dict[str, Any]) -> None:
    if episode.get("source_id") in SUBSTACK_RSS_POST_TRANSCRIPT_SOURCE_IDS:
        _apply_substack_post_transcript_route(episode)
    if episode.get("source_id") in DIRECT_OFFICIAL_PAGE_TRANSCRIPT_HOSTS_BY_SOURCE_ID:
        _apply_direct_official_page_transcript_route(episode)
    if episode.get("source_id") in VERGE_OFFICIAL_TRANSCRIPT_FEEDS:
        _apply_verge_article_transcript_route(episode)
    if episode.get("source_id") != "the-ai-daily-brief":
        return
    if episode.get("verified_transcript_url"):
        return
    published = parse_datetime(episode.get("published_at"))
    if not published:
        return
    local_date = published.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    if local_date < "2026-06-01":
        return
    transcript_url = f"https://aidailybrief.ai/e/{local_date}/transcript.md"
    episode["verified_transcript_url"] = transcript_url
    episode["verified_transcript_type"] = "text/markdown"
    episode["verified_transcript_source_kind"] = "official_show_transcript"
    episode["transcript_url"] = transcript_url
    episode["transcript_type"] = "text/markdown"


def _apply_substack_post_transcript_route(episode: dict[str, Any]) -> None:
    if episode.get("verified_transcript_url"):
        return
    link = episode.get("url")
    if not link:
        return
    parsed = urlparse(str(link))
    if parsed.netloc.lower() not in SUBSTACK_TRANSCRIPT_HOSTS:
        return
    episode["verified_transcript_url"] = str(link)
    episode["verified_transcript_type"] = "text/html"
    episode["verified_transcript_source_kind"] = "official_show_transcript"
    episode["transcript_url"] = str(link)
    episode["transcript_type"] = "text/html"


def _apply_direct_official_page_transcript_route(episode: dict[str, Any]) -> None:
    if episode.get("verified_transcript_url"):
        return
    link = episode.get("url")
    if not link:
        return
    allowed_hosts = DIRECT_OFFICIAL_PAGE_TRANSCRIPT_HOSTS_BY_SOURCE_ID.get(str(episode.get("source_id")), set())
    parsed = urlparse(str(link))
    if parsed.netloc.lower() not in allowed_hosts:
        return
    episode["verified_transcript_url"] = str(link)
    episode["verified_transcript_type"] = "text/html"
    episode["verified_transcript_source_kind"] = "official_show_transcript"
    episode["transcript_url"] = str(link)
    episode["transcript_type"] = "text/html"


def _apply_verge_article_transcript_route(episode: dict[str, Any]) -> None:
    if episode.get("verified_transcript_url"):
        return
    link = episode.get("url")
    if not link:
        return
    parsed = urlparse(str(link))
    if parsed.netloc.lower() not in VERGE_TRANSCRIPT_HOSTS:
        return
    if not parsed.path.startswith("/podcast/"):
        return
    episode["verified_transcript_url"] = str(link)
    episode["verified_transcript_type"] = "text/html"
    episode["verified_transcript_source_kind"] = "official_show_transcript"
    episode["transcript_url"] = str(link)
    episode["transcript_type"] = "text/html"


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
                    LEFT JOIN transcript_acquisition_status AS acquisition_status
                      ON acquisition_status.episode_id = episodes.id
                    WHERE jobs.lane = ?
                      AND jobs.job_type = 'manual_transcript_required'
                      AND jobs.attempts < jobs.max_attempts
                      AND (jobs.status = 'pending' OR (jobs.status = 'claimed' AND jobs.leased_until < ?))
                      AND (acquisition_status.next_eligible_at IS NULL OR acquisition_status.next_eligible_at <= ?)
                  )
                  WHERE active_claims < ?
                    AND source_rank <= (? - active_claims)
                  ORDER BY priority ASC, id ASC
                  LIMIT ?
                )
                RETURNING id
                """,
                (worker_id, leased_until, now, now, lane, now, now, per_source_limit, per_source_limit, effective_limit),
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
                  LEFT JOIN transcript_acquisition_status AS acquisition_status
                    ON acquisition_status.episode_id = episodes.id
                  WHERE jobs.lane = ?
                    AND jobs.job_type = 'manual_transcript_required'
                    AND jobs.attempts < jobs.max_attempts
                    AND (jobs.status = 'pending' OR (jobs.status = 'claimed' AND jobs.leased_until < ?))
                    AND (acquisition_status.next_eligible_at IS NULL OR acquisition_status.next_eligible_at <= ?)
                  ORDER BY jobs.priority ASC, jobs.id ASC
                  LIMIT ?
                )
                RETURNING id
                """,
                (worker_id, leased_until, now, lane, now, now, effective_limit),
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
    params.append(now_iso())
    if job_ids is None:
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
              AND (
                transcript_acquisition_status.next_eligible_at IS NULL
                OR transcript_acquisition_status.next_eligible_at <= ?
              )
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


def _atom_alternate_link(item: ET.Element) -> str | None:
    for link in item.findall(f"{ATOM_NS}link"):
        rel = (link.get("rel") or "alternate").lower()
        link_type = (link.get("type") or "").lower()
        href = link.get("href")
        if href and rel == "alternate" and (not link_type or "html" in link_type):
            return href
    return None


def _attr(item: ET.Element, tag: str, attr: str) -> str | None:
    found = item.find(tag)
    if found is not None:
        return found.get(attr)
    return None


def _transcript_link(item: ET.Element, *, feed_body: str = "") -> tuple[str | None, str | None]:
    candidates: list[tuple[int, str, str | None]] = []
    for element in item.iter():
        if element.tag.endswith("transcript"):
            url = element.get("url") or element.get("href")
            if url:
                transcript_type = element.get("type")
                candidates.append((_transcript_candidate_rank(url, transcript_type, explicit_tag=True), url, transcript_type))
    if not candidates:
        candidates.extend(_transcript_links_from_body(feed_body))
    if not candidates:
        return None, None
    _, url, transcript_type = sorted(candidates, key=lambda candidate: (candidate[0], candidate[1]))[0]
    return url, transcript_type


def _transcript_candidate_rank(url: str, transcript_type: str | None, *, explicit_tag: bool) -> int:
    lowered = f"{transcript_type or ''} {url}".lower()
    if "youtube.com" in lowered or "youtu.be" in lowered:
        return 20 if explicit_tag else 90
    if "vtt" in lowered or "webvtt" in lowered:
        return 0
    if "json" in lowered:
        return 1
    if "text/plain" in lowered or lowered.split("?", 1)[0].endswith(".txt"):
        return 2
    if "subrip" in lowered or lowered.split("?", 1)[0].endswith(".srt"):
        return 3
    if "/transcript" in lowered or "transcript" in lowered:
        return 4
    return 10 if explicit_tag else 50


def _transcript_links_from_body(feed_body: str) -> list[tuple[int, str, str | None]]:
    if not feed_body:
        return []
    links: list[tuple[int, str, str | None]] = []
    for match in TRANSCRIPT_LINK_RE.finditer(feed_body):
        url = html.unescape(match.group(1).strip())
        label = re.sub(r"<[^>]+>", " ", match.group(2))
        window = feed_body[max(0, match.start() - 120) : min(len(feed_body), match.end() + 120)]
        transcript_type = _transcript_type_from_url(url)
        if _body_link_is_allowed_transcript(url, f"{label} {window}", transcript_type):
            links.append((_transcript_candidate_rank(url, transcript_type, explicit_tag=False), url, transcript_type))
    for raw_url in RAW_URL_RE.findall(feed_body):
        url = html.unescape(raw_url.rstrip(").,;"))
        transcript_type = _transcript_type_from_url(url)
        if _body_link_is_direct_transcript(url, transcript_type):
            links.append((_transcript_candidate_rank(url, transcript_type, explicit_tag=False), url, transcript_type))
    deduped: dict[str, tuple[int, str, str | None]] = {}
    for rank, url, transcript_type in links:
        existing = deduped.get(url)
        if existing is None or rank < existing[0]:
            deduped[url] = (rank, url, transcript_type)
    return list(deduped.values())


def _body_link_is_allowed_transcript(url: str, context: str, transcript_type: str | None) -> bool:
    lowered_url = url.lower()
    if "youtube.com" in lowered_url or "youtu.be" in lowered_url:
        return False
    if _body_link_is_direct_transcript(url, transcript_type):
        return True
    lowered_context = context.lower()
    return "transcript" in lowered_context and ("transcript" in lowered_url or "/transcript" in lowered_url)


def _body_link_is_direct_transcript(url: str, transcript_type: str | None) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    if any(path.endswith(ext) for ext in TRANSCRIPT_DIRECT_EXTENSIONS):
        return True
    lowered_type = (transcript_type or "").lower()
    return any(token in lowered_type for token in ["vtt", "subrip", "json", "plain"])


def _transcript_type_from_url(url: str) -> str | None:
    lowered = url.lower()
    path = urlparse(url).path.lower()
    if path.endswith(".vtt") or "format=webvtt" in lowered:
        return "text/vtt"
    if path.endswith(".srt") or "format=subrip" in lowered:
        return "application/x-subrip"
    if path.endswith(".json"):
        return "application/json"
    if path.endswith((".txt", ".md")):
        return "text/plain"
    if path.endswith((".html", ".htm")) or "/transcript" in path:
        return "text/html"
    return None


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
