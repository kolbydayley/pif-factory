"""Bounded acquisition planning for the clean-corpus benchmark rump.

This module is deliberately a planner, not a downloader.  It selects exactly
four episode-disjoint candidates per blocked show and records the permitted
lane order.  Browser work and paid ASR remain explicit external prerequisites.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import os
from pathlib import Path
import re
import sqlite3
from typing import Iterable, Mapping, Sequence

from research_factory.ingest import assert_transcript_plausible


EPISODES_PER_SHOW = 4
NPR_HIBT_INDEX = Path("work/pif-ops/official-transcript-index/npr-hibt.json")
BEN_MARC_CHANNEL = "https://www.youtube.com/@a16z/videos"
MIN_VIDEO_SECONDS = 1500
CHANNEL_SANITY_SAMPLE = 40
CHANNEL_SANITY_MIN_MATCH = 0.25

_STOP = {
    "the", "and", "with", "for", "from", "this", "that", "podcast",
    "episode", "show", "how", "why", "what", "a16z",
}


@dataclass(frozen=True)
class Candidate:
    episode_id: str
    title: str
    published_at: str
    duration_seconds: int | None
    audio_url: str | None
    transcript_url: str | None
    primary_lane: str
    fallback_lane: str | None = None
    video_id: str | None = None


def _tokens(value: str) -> set[str]:
    return {
        token for token in re.findall(r"[a-z0-9]+", (value or "").lower())
        if len(token) > 2 and token not in _STOP
    }


def _jaccard(left: set[str], right: set[str]) -> float:
    return len(left & right) / len(left | right) if left and right else 0.0


def _episode_containment(episode: set[str], candidate: set[str]) -> float:
    return len(episode & candidate) / len(episode) if episode else 0.0


def _rows(conn: sqlite3.Connection, source_id: str) -> list[sqlite3.Row]:
    previous = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            """SELECT id, title, published_at, duration_seconds, audio_url,
                      verified_transcript_url
                 FROM episodes
                WHERE source_id = ? AND published_at IS NOT NULL
                ORDER BY published_at, id""",
            (source_id,),
        ).fetchall()
    finally:
        conn.row_factory = previous


def select_period_spread(rows: Sequence[sqlite3.Row], *, count: int = 4) -> list[sqlite3.Row]:
    """Select deterministic chronological anchors across the complete catalog."""
    if len(rows) < count:
        raise ValueError(f"need {count} catalog episodes; found {len(rows)}")
    indexes = [round(position * (len(rows) - 1) / (count - 1)) for position in range(count)]
    selected = [rows[index] for index in indexes]
    if len({row["id"] for row in selected}) != count:
        raise ValueError("period-spread selection did not produce episode-disjoint candidates")
    return selected


def channel_sanity(
    catalog_rows: Sequence[sqlite3.Row],
    listing: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Apply the pre-existing 40-episode/25% creator-channel sanity gate."""
    sample = list(reversed(catalog_rows))[:CHANNEL_SANITY_SAMPLE]
    eligible_videos = [
        video for video in listing
        if isinstance(video.get("duration"), (int, float))
        and float(video["duration"]) >= MIN_VIDEO_SECONDS
    ]
    video_tokens = [_tokens(str(video.get("title") or "")) for video in eligible_videos]
    hits = sum(
        any(_jaccard(_tokens(row["title"]), candidate) >= 0.5 for candidate in video_tokens)
        for row in sample
    )
    denominator = len(sample)
    score = hits / denominator if denominator else 0.0
    return {
        "sample_size": denominator,
        "required_sample_size": CHANNEL_SANITY_SAMPLE,
        "matching_episodes": hits,
        "score": score,
        "threshold": CHANNEL_SANITY_MIN_MATCH,
        "passed": denominator == CHANNEL_SANITY_SAMPLE and score >= CHANNEL_SANITY_MIN_MATCH,
        "duration_floor_seconds": MIN_VIDEO_SECONDS,
    }


def _caption_matches(
    rows: Sequence[sqlite3.Row],
    listing: Sequence[Mapping[str, object]],
) -> list[tuple[sqlite3.Row, Mapping[str, object]]]:
    videos = [
        video for video in listing
        if isinstance(video.get("duration"), (int, float))
        and float(video["duration"]) >= MIN_VIDEO_SECONDS
    ]
    matches: list[tuple[sqlite3.Row, Mapping[str, object]]] = []
    used: set[str] = set()
    for row in rows:
        episode_tokens = _tokens(row["title"])
        ranked = sorted(
            (
                (_episode_containment(episode_tokens, _tokens(str(video.get("title") or ""))), video)
                for video in videos
                if str(video.get("id") or "") not in used
            ),
            key=lambda item: (item[0], str(item[1].get("id") or "")),
            reverse=True,
        )
        if ranked and ranked[0][0] >= 0.75:
            video = ranked[0][1]
            used.add(str(video["id"]))
            matches.append((row, video))
    return matches


def _candidate(row: sqlite3.Row, primary: str, fallback: str | None = None, *,
               transcript_url: str | None = None, video_id: str | None = None) -> Candidate:
    return Candidate(
        episode_id=row["id"],
        title=row["title"],
        published_at=row["published_at"],
        duration_seconds=row["duration_seconds"],
        audio_url=row["audio_url"],
        transcript_url=transcript_url,
        primary_lane=primary,
        fallback_lane=fallback,
        video_id=video_id,
    )


def _require_exact_four(candidates: Iterable[Candidate], show: str) -> list[Candidate]:
    result = list(candidates)
    if len(result) != EPISODES_PER_SHOW or len({c.episode_id for c in result}) != EPISODES_PER_SHOW:
        raise ValueError(f"{show}: acquisition plan must contain exactly four distinct episodes")
    return result


def plan_blocked_show_acquisition(
    conn: sqlite3.Connection,
    *,
    youtube_listing: Sequence[Mapping[str, object]] | None = None,
    groq_api_key: str | None = None,
) -> dict[str, object]:
    """Return the bounded five-show benchmark plan without performing I/O."""
    key_available = bool(groq_api_key or os.environ.get("GROQ_API_KEY"))
    plans: dict[str, dict[str, object]] = {}

    hibt_rows = [
        row for row in _rows(conn, "how-i-built-this")
        if str(row["verified_transcript_url"] or "").startswith("https://www.npr.org/transcripts/")
    ]
    hibt = _require_exact_four(
        (_candidate(
            row,
            "browser_npr",
            "groq_asr",
            transcript_url=row["verified_transcript_url"],
        )
         for row in select_period_spread(hibt_rows)),
        "how-i-built-this",
    )
    plans["how-i-built-this"] = {
        "candidates": [asdict(c) for c in hibt],
        "browser_required": True,
        "asr_fallback_requires_groq": True,
        "index_artifact": str(NPR_HIBT_INDEX),
        "notes": "Client-rendered NPR transcript pages; plain HTTP fetch is forbidden.",
    }

    marketplace_rows = _rows(conn, "marketplace-tech")
    marketplace = _require_exact_four(
        (_candidate(row, "browser_marketplace", "groq_asr")
         for row in select_period_spread(marketplace_rows)),
        "marketplace-tech",
    )
    plans["marketplace-tech"] = {
        "candidates": [asdict(c) for c in marketplace],
        "browser_required": True,
        "asr_fallback_requires_groq": True,
    }

    for source_id in ("search-engine", "tech-brew-ride-home"):
        audio_rows = [row for row in _rows(conn, source_id) if row["audio_url"]]
        chosen = _require_exact_four(
            (_candidate(row, "groq_asr") for row in select_period_spread(audio_rows)),
            source_id,
        )
        plans[source_id] = {
            "candidates": [asdict(c) for c in chosen],
            "asr_requires_groq": True,
        }

    ben_rows = _rows(conn, "the-ben-and-marc-show")
    sanity = channel_sanity(ben_rows, youtube_listing or [])
    matches = _caption_matches(ben_rows, youtube_listing or []) if sanity["passed"] else []
    if len(matches) >= EPISODES_PER_SHOW:
        spread = select_period_spread([row for row, _ in matches])
        by_episode = {row["id"]: video for row, video in matches}
        ben = _require_exact_four(
            (_candidate(
                row,
                "youtube_caption_browser",
                "groq_asr",
                transcript_url=f"https://www.youtube.com/watch?v={by_episode[row['id']]['id']}",
                video_id=str(by_episode[row["id"]]["id"]),
            ) for row in spread),
            "the-ben-and-marc-show",
        )
    else:
        ben = _require_exact_four(
            (_candidate(row, "youtube_caption_discovery", "groq_asr")
             for row in select_period_spread(ben_rows)),
            "the-ben-and-marc-show",
        )
    plans["the-ben-and-marc-show"] = {
        "candidates": [asdict(c) for c in ben],
        "channel": BEN_MARC_CHANNEL,
        "channel_sanity": sanity,
        "caption_first": True,
        "asr_fallback_requires_groq": True,
    }

    asr_only = ["search-engine", "tech-brew-ride-home"]
    prerequisites = []
    if not key_available:
        prerequisites.append({
            "code": "missing_groq_api_key",
            "environment_variable": "GROQ_API_KEY",
            "blocks": asr_only,
            "conditionally_blocks_fallback_for": [
                "how-i-built-this", "marketplace-tech", "the-ben-and-marc-show",
            ],
            "message": (
                "GROQ_API_KEY is required for the two ASR-only benchmark shows. "
                "It is also required if HIBT or Marketplace browser acquisition, "
                "or the Ben and Marc caption lane, fails. "
                "Acquire exactly four episodes per show; full-catalog ASR is out of scope."
            ),
        })
    return {
        "schema_version": 1,
        "episodes_per_show": EPISODES_PER_SHOW,
        "full_catalog_asr_allowed": False,
        # This is deliberately not an acquisition-readiness receipt: browser
        # and caption outcomes have not happened yet. It only reports whether
        # the external ASR credential prerequisite is currently satisfied.
        "asr_credential_available": key_available,
        "ready_for_all_five": False,
        "prerequisites": prerequisites,
        "shows": plans,
    }


def validate_transcript_ingest(*, text: str, duration_seconds: int | None,
                               episode_id: str) -> None:
    """Apply the canonical shell-poison guard before benchmark fixture storage."""
    assert_transcript_plausible(
        words=len(text.split()),
        duration_seconds=duration_seconds,
        episode_id=episode_id,
    )
