from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .paths import root
from .util import slugify


DEFAULT_STRATEGY_PATH = root() / "config" / "transcript_strategies.json"


def load_transcript_strategies(path: str | Path | None = None) -> list[dict[str, Any]]:
    strategy_path = Path(path).expanduser().resolve() if path else DEFAULT_STRATEGY_PATH
    data = json.loads(strategy_path.read_text(encoding="utf-8"))
    strategies = data.get("strategies")
    if not isinstance(strategies, list):
        raise ValueError(f"Transcript strategy file must contain a strategies list: {strategy_path}")
    normalized = []
    for strategy in strategies:
        if not isinstance(strategy, dict):
            raise ValueError(f"Transcript strategy entries must be objects: {strategy_path}")
        if not strategy.get("id"):
            raise ValueError(f"Transcript strategy missing id: {strategy_path}")
        sources = strategy.get("sources")
        if not isinstance(sources, list) or not sources:
            raise ValueError(f"Transcript strategy missing sources: {strategy['id']}")
        normalized.append(strategy)
    return normalized


def transcript_strategy_report(conn, *, strategy_path: str | Path | None = None) -> dict[str, Any]:
    strategies = load_transcript_strategies(strategy_path)
    source_counts = _source_counts(conn)
    rows = []
    totals = {
        "sources": 0,
        "episodes": 0,
        "feed_transcript_links": 0,
        "ready_transcripts": 0,
        "quarantined_transcripts": 0,
        "linked_not_ready": 0,
        "manual_pending": 0,
    }
    for strategy in strategies:
        counts = _empty_counts()
        source_rows = []
        for source_name in strategy["sources"]:
            source_key = _source_key(source_name)
            source_row = source_counts.get(source_key) or source_counts.get(slugify(source_name))
            if not source_row:
                source_rows.append({"source": source_name, "found": False})
                continue
            counts = _merge_counts(counts, source_row)
            source_rows.append({"source": source_name, "found": True, **source_row})
        for key in totals:
            totals[key] += counts.get(key, 0)
        rows.append(
            {
                "id": strategy["id"],
                "lane": strategy.get("lane"),
                "route_type": strategy.get("route_type"),
                "status": strategy.get("status"),
                "priority": strategy.get("priority"),
                "expected_yield": strategy.get("expected_yield"),
                "automation": strategy.get("automation"),
                "failure_rule": strategy.get("failure_rule"),
                "counts": counts,
                "sources": source_rows,
            }
        )
    return {
        "ok": True,
        "strategy_count": len(strategies),
        "totals": totals,
        "strategies": rows,
        "privacy": "sanitized_operational_report_no_raw_transcripts",
    }


def _source_counts(conn) -> dict[str, dict[str, int | str]]:
    rows = conn.execute(
        """
        SELECT
          sources.id AS source_id,
          sources.name AS source_name,
          COUNT(DISTINCT episodes.id) AS episodes,
          COUNT(DISTINCT CASE WHEN episodes.feed_transcript_url IS NOT NULL THEN episodes.id END) AS feed_transcript_links,
          COUNT(DISTINCT CASE WHEN transcripts.status = 'ready' THEN transcripts.id END) AS ready_transcripts,
          COUNT(DISTINCT CASE WHEN transcripts.status = 'quarantined' THEN transcripts.id END) AS quarantined_transcripts,
          COUNT(DISTINCT CASE
            WHEN episodes.feed_transcript_url IS NOT NULL
             AND NOT EXISTS (
               SELECT 1
               FROM transcripts AS ready_transcripts
               WHERE ready_transcripts.episode_id = episodes.id
                 AND ready_transcripts.status = 'ready'
             )
            THEN episodes.id
          END) AS linked_not_ready,
          COUNT(DISTINCT CASE
            WHEN jobs.job_type = 'manual_transcript_required'
             AND jobs.status = 'pending'
            THEN jobs.id
          END) AS manual_pending
        FROM sources
        LEFT JOIN episodes ON episodes.source_id = sources.id
        LEFT JOIN transcripts ON transcripts.episode_id = episodes.id
        LEFT JOIN jobs ON jobs.target_id = episodes.id
        GROUP BY sources.id
        """
    ).fetchall()
    counts: dict[str, dict[str, int | str]] = {}
    for row in rows:
        item = {
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "sources": 1,
            "episodes": int(row["episodes"] or 0),
            "feed_transcript_links": int(row["feed_transcript_links"] or 0),
            "ready_transcripts": int(row["ready_transcripts"] or 0),
            "quarantined_transcripts": int(row["quarantined_transcripts"] or 0),
            "linked_not_ready": int(row["linked_not_ready"] or 0),
            "manual_pending": int(row["manual_pending"] or 0),
        }
        counts[_source_key(row["source_name"])] = item
        counts[_source_key(row["source_id"])] = item
    return counts


def _empty_counts() -> dict[str, int]:
    return {
        "sources": 0,
        "episodes": 0,
        "feed_transcript_links": 0,
        "ready_transcripts": 0,
        "quarantined_transcripts": 0,
        "linked_not_ready": 0,
        "manual_pending": 0,
    }


def _merge_counts(left: dict[str, int], right: dict[str, Any]) -> dict[str, int]:
    merged = dict(left)
    for key in merged:
        merged[key] += int(right.get(key, 0) or 0)
    return merged


def _source_key(value: str) -> str:
    return slugify(value).casefold()
