"""Frozen production-cohort loading and database validation."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from .paths import root
from .util import dumps_json, sha256_text


DEFAULT_COHORT_PATH = root() / "config" / "production_cohort_v1.json"
COHORT_SCHEMA_VERSION = "pif_production_cohort_v1"
EXPECTED_EPISODES = 25
EXPECTED_SHOWS = 5


class CohortValidationError(ValueError):
    pass


def load_production_cohort(path: str | Path | None = None) -> dict[str, Any]:
    cohort_path = Path(path or DEFAULT_COHORT_PATH).expanduser().resolve()
    try:
        payload = json.loads(cohort_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CohortValidationError(f"cannot load production cohort: {cohort_path}") from exc
    if not isinstance(payload, Mapping):
        raise CohortValidationError("production cohort must be a JSON object")
    cohort = dict(payload)
    if cohort.get("schema_version") != COHORT_SCHEMA_VERSION:
        raise CohortValidationError(f"production cohort must use {COHORT_SCHEMA_VERSION}")
    episodes = cohort.get("episodes")
    if not isinstance(episodes, list) or len(episodes) != EXPECTED_EPISODES:
        raise CohortValidationError(f"production cohort must contain exactly {EXPECTED_EPISODES} episodes")
    ids: list[str] = []
    source_counts: Counter[str] = Counter()
    for index, item in enumerate(episodes):
        if not isinstance(item, Mapping):
            raise CohortValidationError(f"cohort episode {index} must be an object")
        episode_id = item.get("id")
        source_id = item.get("source_id")
        published_at = item.get("published_at")
        if not all(isinstance(value, str) and value.strip() for value in (episode_id, source_id, published_at)):
            raise CohortValidationError(f"cohort episode {index} has incomplete identity fields")
        ids.append(str(episode_id))
        source_counts[str(source_id)] += 1
    if len(set(ids)) != EXPECTED_EPISODES:
        raise CohortValidationError("production cohort episode IDs must be unique")
    if len(source_counts) != EXPECTED_SHOWS or set(source_counts.values()) != {5}:
        raise CohortValidationError("production cohort must contain five episodes from each of five shows")
    cohort["path"] = str(cohort_path)
    cohort["sha256"] = sha256_text(dumps_json(payload))
    cohort["episode_ids"] = ids
    return cohort


def verify_production_cohort(
    conn: sqlite3.Connection,
    *,
    path: str | Path | None = None,
    label_pack: str = "ai_discourse_v3_1",
    model: str = "gpt-5.5",
) -> dict[str, Any]:
    """Fail closed unless all frozen episodes have the accepted extraction backbone."""

    cohort = load_production_cohort(path)
    episode_items = {str(item["id"]): item for item in cohort["episodes"]}
    episode_ids = cohort["episode_ids"]
    placeholders = ",".join("?" for _ in episode_ids)
    rows = conn.execute(
        f"SELECT id, source_id, published_at FROM episodes WHERE id IN ({placeholders})",
        episode_ids,
    ).fetchall()
    found = {str(row["id"]): row for row in rows}
    errors: list[str] = []
    for episode_id, expected in episode_items.items():
        row = found.get(episode_id)
        if row is None:
            errors.append(f"missing episode {episode_id}")
            continue
        if row["source_id"] != expected["source_id"]:
            errors.append(f"source changed for {episode_id}")
        if row["published_at"] != expected["published_at"]:
            errors.append(f"published_at changed for {episode_id}")

    readiness = conn.execute(
        f"""
        SELECT
          COUNT(DISTINCT CASE WHEN transcripts.status = 'ready' THEN episodes.id END) AS transcript_episodes,
          COUNT(DISTINCT CASE WHEN context_runs.status = 'completed' THEN episodes.id END) AS context_episodes,
          COUNT(DISTINCT CASE WHEN labels.status IN ('ready', 'completed', 'accepted') THEN episodes.id END) AS label_episodes,
          COUNT(DISTINCT CASE WHEN discourse_events.id IS NOT NULL THEN episodes.id END) AS event_episodes,
          COUNT(DISTINCT CASE WHEN trim(COALESCE(discourse_events.claim_text, '')) <> '' THEN discourse_events.id END) AS claim_events
        FROM episodes
        LEFT JOIN transcripts
          ON transcripts.episode_id = episodes.id
        LEFT JOIN episode_context_runs AS context_runs
          ON context_runs.episode_id = episodes.id
         AND context_runs.label_pack = ?
         AND context_runs.model = ?
        LEFT JOIN segments ON segments.episode_id = episodes.id
        LEFT JOIN labels
          ON labels.segment_id = segments.id
         AND labels.label_pack = ?
        LEFT JOIN discourse_events ON discourse_events.label_id = labels.id
        WHERE episodes.id IN ({placeholders})
        """,
        (label_pack, model, label_pack, *episode_ids),
    ).fetchone()
    expected = EXPECTED_EPISODES
    for key in ("transcript_episodes", "context_episodes", "label_episodes", "event_episodes"):
        if int(readiness[key] or 0) != expected:
            errors.append(f"{key}={int(readiness[key] or 0)} expected={expected}")
    if int(readiness["claim_events"] or 0) < 200:
        errors.append(f"claim_events={int(readiness['claim_events'] or 0)} expected_at_least=200")
    return {
        "ok": not errors,
        "cohort_id": cohort["cohort_id"],
        "cohort_path": cohort["path"],
        "cohort_sha256": cohort["sha256"],
        "episode_ids": episode_ids,
        "episode_count": len(episode_ids),
        "show_count": len({item["source_id"] for item in cohort["episodes"]}),
        "transcript_episodes": int(readiness["transcript_episodes"] or 0),
        "context_episodes": int(readiness["context_episodes"] or 0),
        "label_episodes": int(readiness["label_episodes"] or 0),
        "event_episodes": int(readiness["event_episodes"] or 0),
        "claim_events": int(readiness["claim_events"] or 0),
        "errors": errors,
    }


__all__ = [
    "COHORT_SCHEMA_VERSION",
    "CohortValidationError",
    "DEFAULT_COHORT_PATH",
    "EXPECTED_EPISODES",
    "EXPECTED_SHOWS",
    "load_production_cohort",
    "verify_production_cohort",
]
