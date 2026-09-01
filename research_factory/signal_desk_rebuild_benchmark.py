"""Private benchmark fixtures and transcript-acquisition matching helpers.

The out-of-domain (OOD) corpus is evaluation material, not production data.
It therefore lives in a separate SQLite file with deliberately different
table names.  Signal Desk aggregation receives only the canonical factory
connection and cannot discover or join this database unless a caller makes
an explicit, prohibited ``ATTACH``.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit


SCHEMA_VERSION = "signal-desk-benchmark-store-v1"
BENCHMARK_SOURCE_NAMESPACE = "signal_desk_benchmark_only"
TRACKING_WRAPPER_HOSTS = frozenset({
    "rss.pdrl.fm",
    "pdst.fm",
    "pscrb.fm",
    "chrt.fm",
    "chtbl.com",
    "podtrac.com",
    "www.podtrac.com",
    "pdcst.fm",
    "mgln.ai",
    "tracking.swap.fm",
})
_CANONICAL_TABLE_NAMES = frozenset({"episodes", "segments", "transcripts"})
_URL_QUERY_KEYS = ("url", "u", "target", "redirect", "destination", "dest")
_TOKEN_RE = re.compile(r"[a-z0-9]+")


class BenchmarkIsolationError(ValueError):
    """Raised when benchmark storage could overlap production storage."""


def _resolved(path: Path | str) -> Path:
    return Path(path).expanduser().resolve()


def assert_benchmark_path(benchmark_db: Path | str,
                          canonical_db: Path | str) -> None:
    """Fail closed if the benchmark could overwrite the canonical database."""
    benchmark = _resolved(benchmark_db)
    canonical = _resolved(canonical_db)
    if benchmark == canonical:
        raise BenchmarkIsolationError(
            "benchmark database must not be the canonical factory database"
        )
    # Prevent a misleading sidecar such as ``factory.sqlite.ood`` beside the
    # production database.  Evaluation fixtures belong in their own tree.
    if benchmark.parent == canonical.parent:
        raise BenchmarkIsolationError(
            "benchmark database must live outside the canonical data directory"
        )


def initialize_benchmark_store(benchmark_db: Path | str,
                               canonical_db: Path | str) -> sqlite3.Connection:
    """Create/open the physically separate private OOD fixture store."""
    assert_benchmark_path(benchmark_db, canonical_db)
    path = _resolved(benchmark_db)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS benchmark_metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS benchmark_shows (
          benchmark_show_id TEXT PRIMARY KEY,
          display_name TEXT NOT NULL,
          cohort TEXT NOT NULL CHECK (
            cohort IN ('claim_dense', 'narrative', 'format_stress')
          ),
          source_namespace TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS benchmark_episodes (
          benchmark_episode_id TEXT PRIMARY KEY,
          benchmark_show_id TEXT NOT NULL REFERENCES benchmark_shows(benchmark_show_id),
          title TEXT NOT NULL,
          published_at TEXT,
          official_url TEXT NOT NULL,
          duration_seconds REAL,
          transcript_sha256 TEXT NOT NULL,
          transcript_text TEXT NOT NULL,
          source_kind TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS benchmark_windows (
          benchmark_window_id TEXT PRIMARY KEY,
          benchmark_episode_id TEXT NOT NULL
            REFERENCES benchmark_episodes(benchmark_episode_id),
          window_index INTEGER NOT NULL,
          split TEXT NOT NULL CHECK (split IN ('development', 'validation', 'holdout')),
          text_sha256 TEXT NOT NULL,
          text TEXT NOT NULL,
          UNIQUE (benchmark_episode_id, window_index)
        );
        """
    )
    conn.execute(
        "INSERT OR REPLACE INTO benchmark_metadata(key, value) VALUES (?, ?)",
        ("schema_version", SCHEMA_VERSION),
    )
    conn.execute(
        "INSERT OR REPLACE INTO benchmark_metadata(key, value) VALUES (?, ?)",
        ("source_namespace", BENCHMARK_SOURCE_NAMESPACE),
    )
    forbidden = {
        row["name"] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ) if row["name"] in _CANONICAL_TABLE_NAMES
    }
    if forbidden:
        conn.close()
        raise BenchmarkIsolationError(
            f"benchmark store contains canonical tables: {sorted(forbidden)}"
        )
    conn.commit()
    return conn


def put_benchmark_transcript(
    conn: sqlite3.Connection,
    *,
    show_id: str,
    show_name: str,
    cohort: str,
    episode_id: str,
    title: str,
    official_url: str,
    transcript_text: str,
    source_kind: str = "official_transcript",
    published_at: str | None = None,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    """Freeze one private transcript and return its immutable receipt."""
    text = transcript_text.strip()
    if not text:
        raise ValueError("benchmark transcript cannot be empty")
    if cohort not in {"claim_dense", "narrative", "format_stress"}:
        raise ValueError(
            "cohort must be claim_dense, narrative, or format_stress"
        )
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    conn.execute(
        """INSERT OR IGNORE INTO benchmark_shows
           (benchmark_show_id, display_name, cohort, source_namespace)
           VALUES (?, ?, ?, ?)""",
        (show_id, show_name, cohort, BENCHMARK_SOURCE_NAMESPACE),
    )
    existing = conn.execute(
        "SELECT transcript_sha256 FROM benchmark_episodes "
        "WHERE benchmark_episode_id = ?", (episode_id,),
    ).fetchone()
    if existing and existing["transcript_sha256"] != digest:
        raise ValueError("frozen benchmark transcript content changed")
    conn.execute(
        """INSERT OR IGNORE INTO benchmark_episodes
           (benchmark_episode_id, benchmark_show_id, title, published_at,
            official_url, duration_seconds, transcript_sha256, transcript_text,
            source_kind)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (episode_id, show_id, title, published_at, official_url,
         duration_seconds, digest, text, source_kind),
    )
    conn.commit()
    return {
        "benchmark_episode_id": episode_id,
        "source_namespace": BENCHMARK_SOURCE_NAMESPACE,
        "transcript_sha256": digest,
    }


def _embedded_url(value: str) -> str | None:
    decoded = unquote(value)
    matches = list(re.finditer(r"https?://", decoded, flags=re.IGNORECASE))
    if len(matches) < 2:
        return None
    return decoded[matches[-1].start():]


def unwrap_tracking_url(value: str, *, max_hops: int = 5) -> str:
    """Recover a feed/media URL embedded by known podcast click trackers.

    This is a syntactic unwrap only and performs no network request. Unknown
    hosts are returned unchanged, which prevents arbitrary query parameters
    from being treated as authoritative episode URLs.
    """
    current = value.strip()
    for _ in range(max_hops):
        try:
            parts = urlsplit(current)
        except ValueError:
            return current
        host = (parts.hostname or "").casefold()
        if host not in TRACKING_WRAPPER_HOSTS:
            return current
        candidate: str | None = None
        query = parse_qs(parts.query)
        for key in _URL_QUERY_KEYS:
            values = query.get(key)
            if values and values[-1].startswith(("http://", "https://")):
                candidate = values[-1]
                break
        if candidate is None:
            candidate = _embedded_url(current)
        if not candidate or candidate == current:
            return current
        current = candidate
    return current


def episode_title_tokens(title: str) -> frozenset[str]:
    """Normalize title tokens while retaining episode-specific numbers."""
    normalized = unicodedata.normalize("NFKD", title).encode(
        "ascii", "ignore"
    ).decode("ascii").casefold()
    return frozenset(_TOKEN_RE.findall(normalized))


def episode_side_token_containment(episode_title: str,
                                   transcript_page_title: str) -> float:
    """Fraction of episode-title tokens found on the transcript page.

    The denominator is intentionally only the episode title. Official pages
    append descriptions, site names, and anchors, so symmetric Jaccard would
    incorrectly penalize valid matches.
    """
    episode_tokens = episode_title_tokens(episode_title)
    if not episode_tokens:
        return 0.0
    page_tokens = episode_title_tokens(transcript_page_title)
    return len(episode_tokens & page_tokens) / len(episode_tokens)


def transcript_page_matches_episode(episode_title: str,
                                    transcript_page_title: str,
                                    *, threshold: float = 0.85) -> bool:
    if not 0 <= threshold <= 1:
        raise ValueError("threshold must be between 0 and 1")
    return episode_side_token_containment(
        episode_title, transcript_page_title
    ) >= threshold


def benchmark_manifest(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return a hashable private inventory without transcript text."""
    rows = [dict(row) for row in conn.execute(
        """SELECT e.benchmark_episode_id, e.benchmark_show_id, s.display_name,
                  s.cohort, e.title, e.published_at, e.official_url,
                  e.transcript_sha256, e.source_kind
           FROM benchmark_episodes e
           JOIN benchmark_shows s USING (benchmark_show_id)
           ORDER BY e.benchmark_show_id, e.benchmark_episode_id"""
    )]
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return {
        "schema_version": SCHEMA_VERSION,
        "source_namespace": BENCHMARK_SOURCE_NAMESPACE,
        "episodes": rows,
        "manifest_sha256": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
    }
