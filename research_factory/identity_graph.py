from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter, defaultdict
from typing import Any

from .util import dumps_json, now_iso, stable_id


ENTITY_TABLES = {
    "person": ("canonical_people", "cp_"),
    "org": ("canonical_orgs", "co_"),
    "product": ("canonical_products", "cprod_"),
    "model": ("canonical_models", "cmod_"),
}

GENERIC_IDENTITY_NAMES = {
    "ai",
    "agent",
    "agents",
    "article",
    "article_page",
    "article_page_show_copy",
    "blog",
    "company",
    "customer",
    "customers",
    "developer",
    "developers",
    "editorial",
    "editorial_narrator",
    "guest",
    "host",
    "human",
    "humans",
    "interviewer",
    "listener",
    "listeners",
    "model",
    "people",
    "person",
    "podcast",
    "page",
    "page_copy",
    "researcher",
    "speaker",
    "show",
    "show_copy",
    "team",
    "unknown",
    "user",
    "users",
    "we",
}

KNOWN_IDENTITY_ALIAS_NORMALIZATIONS = {
    "ron_roy": "ranjan_roy",
    "ran_john_roy": "ranjan_roy",
    "ranjan_roy_of_margins": "ranjan_roy",
    "alex": "alex_kantrowitz",
    "alex_kantrowitz_big_technology": "alex_kantrowitz",
    "brandon_anderson_latent_space": "brandon_anderson",
    "brandon_anderson_latent_space_podcast": "brandon_anderson",
}


def groom_identity_graph(conn, *, model: str, pilot_id: str | None = None, limit: int | None = None) -> dict[str, Any]:
    ts = now_iso()
    run_id = stable_id("identity_graph", pilot_id or "all", model, ts, prefix="sir_")
    conn.execute(
        """
        INSERT INTO speaker_identity_runs
          (id, run_type, model, status, parameters_json, metrics_json, created_at, updated_at)
        VALUES (?, 'deterministic_candidate_bootstrap', ?, 'running', ?, '{}', ?, ?)
        """,
        (run_id, model, dumps_json({"pilot_id": pilot_id, "limit": limit}), ts, ts),
    )

    event_filter = _pilot_event_filter(conn, pilot_id)
    speaker_rows = _speaker_rows(conn, event_filter=event_filter, limit=limit)
    actor_rows = _actor_rows(conn, event_filter=event_filter, limit=limit)

    metrics: Counter[str] = Counter()
    person_lookup: dict[str, str] = {}
    entity_lookup: dict[tuple[str, str], str] = {}

    for row in speaker_rows:
        normalized = normalize_identity_name(row["surface_name"])
        if not _usable_identity(normalized):
            _ignore_speaker_row(conn, row)
            metrics["speaker_mentions_ignored_low_value"] += 1
            continue
        entity_id = _upsert_entity(
            conn,
            entity_type="person",
            display_name=row["surface_name"],
            normalized_name=normalized,
            confidence=_confidence(row["confidence"], floor=0.6, cap=0.82),
            evidence=_speaker_evidence(row),
            ts=ts,
        )
        person_lookup[normalized] = entity_id
        entity_lookup[("person", normalized)] = entity_id
        _upsert_alias(conn, entity_type="person", entity_id=entity_id, alias=row["surface_name"], evidence=_speaker_evidence(row), ts=ts)
        _insert_resolution_candidate(
            conn,
            raw_table="raw_speaker_mentions",
            raw_id=row["id"],
            entity_type="person",
            entity_id=entity_id,
            display_name=row["surface_name"],
            confidence=_confidence(row["confidence"], floor=0.6, cap=0.82),
            model=model,
            evidence=_speaker_evidence(row),
            ts=ts,
        )
        conn.execute(
            """
            UPDATE raw_speaker_mentions
            SET canonical_person_id = ?, resolution_status = 'candidate_match'
            WHERE id = ? AND resolution_status IN ('unresolved', 'candidate_match')
            """,
            (entity_id, row["id"]),
        )
        metrics["speaker_mentions_candidate_matched"] += 1
        if _is_guest_like(row["role"]):
            _upsert_guest_edge(conn, row=row, canonical_person_id=entity_id, ts=ts)
            metrics["podcast_guest_edges_upserted"] += 1

    for row in actor_rows:
        entity_type = _canonical_actor_type(row["canonical_entity_type"], row["mention_type"])
        if not entity_type:
            metrics["actor_mentions_left_unresolved"] += 1
            continue
        normalized = normalize_identity_name(row["surface_name"])
        if not _usable_identity(normalized):
            _ignore_actor_row(conn, row)
            metrics["actor_mentions_ignored_low_value"] += 1
            continue
        entity_id = entity_lookup.get((entity_type, normalized))
        if not entity_id:
            entity_id = _upsert_entity(
                conn,
                entity_type=entity_type,
                display_name=row["surface_name"],
                normalized_name=normalized,
                confidence=_confidence(row["confidence"], floor=0.55, cap=0.8),
                evidence=_actor_evidence(row),
                ts=ts,
            )
            entity_lookup[(entity_type, normalized)] = entity_id
            if entity_type == "person":
                person_lookup[normalized] = entity_id
        _upsert_alias(conn, entity_type=entity_type, entity_id=entity_id, alias=row["surface_name"], evidence=_actor_evidence(row), ts=ts)
        _insert_resolution_candidate(
            conn,
            raw_table="raw_actor_mentions",
            raw_id=row["id"],
            entity_type=entity_type,
            entity_id=entity_id,
            display_name=row["surface_name"],
            confidence=_confidence(row["confidence"], floor=0.55, cap=0.8),
            model=model,
            evidence=_actor_evidence(row),
            ts=ts,
        )
        conn.execute(
            """
            UPDATE raw_actor_mentions
            SET canonical_entity_type = ?, canonical_entity_id = ?, resolution_status = 'candidate_match'
            WHERE id = ? AND resolution_status IN ('unresolved', 'candidate_match')
            """,
            (entity_type, entity_id, row["id"]),
        )
        metrics["actor_mentions_candidate_matched"] += 1

    metrics.update(_build_person_concept_edges(conn, event_filter=event_filter, ts=ts))
    metrics.update(_build_person_person_mentions(conn, person_lookup=person_lookup, event_filter=event_filter, ts=ts))
    score_run_id = _build_authority_scores(conn, graph_run_model=model, event_filter=event_filter, ts=ts)
    metrics["graph_score_runs_created"] += 1

    conn.execute(
        """
        UPDATE speaker_identity_runs
        SET status = 'completed', metrics_json = ?, updated_at = ?
        WHERE id = ?
        """,
        (dumps_json(dict(metrics)), ts, run_id),
    )
    conn.commit()
    return {
        "ok": True,
        "run_id": run_id,
        "graph_score_run_id": score_run_id,
        "model": model,
        "pilot_id": pilot_id,
        "review_status": "deterministic_candidate_bootstrap_pending_gpt55_judge",
        "metrics": dict(metrics),
    }


def normalize_identity_name(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode()
    text = re.sub(r"^@", "", text.strip().lower())
    text = re.sub(r"\b(?:of|from)\s+margins\b", " ", text)
    text = re.sub(r"\b(?:article page|show copy|page copy|editorial narrator|show notes author)\b", " ", text)
    text = re.sub(r"\s*/\s*(?:latent space|big technology|practical ai|dwarkesh podcast|microsoft research podcast)\b.*$", " ", text)
    text = re.sub(r"\b(?:podcast|show|episode|newsletter|blog)\b$", " ", text)
    text = re.sub(r"\b(the|a|an)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    text = KNOWN_IDENTITY_ALIAS_NORMALIZATIONS.get(text, text)
    return text


def _usable_identity(normalized: str) -> bool:
    if not normalized or normalized in GENERIC_IDENTITY_NAMES:
        return False
    return bool(re.search(r"[a-z]", normalized))


def _canonical_actor_type(value: str | None, mention_type: str | None) -> str | None:
    raw = str(value or "").strip().lower()
    if raw in ENTITY_TABLES:
        return raw
    mention = str(mention_type or "").strip().lower()
    if mention in {"person_reference", "claim_actor"}:
        return "person"
    if mention == "org_reference":
        return "org"
    if mention == "product_reference":
        return "product"
    if mention == "model_reference":
        return "model"
    return None


def _confidence(value: Any, *, floor: float, cap: float) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = floor
    return round(max(floor, min(cap, numeric)), 3)


def _pilot_event_filter(conn, pilot_id: str | None) -> list[str] | None:
    if not pilot_id:
        return None
    rows = conn.execute(
        """
        SELECT DISTINCT discourse_events.id
        FROM discourse_events
        JOIN labels ON labels.id = discourse_events.label_id
        JOIN jobs ON jobs.target_id = labels.segment_id
        WHERE json_extract(jobs.payload_json, '$.pilot_id') = ?
          AND jobs.job_type = 'label_segment'
        """,
        (pilot_id,),
    ).fetchall()
    return [row["id"] for row in rows]


def _speaker_rows(conn, *, event_filter: list[str] | None, limit: int | None):
    sql = """
        SELECT raw_speaker_mentions.*, episodes.source_id
        FROM raw_speaker_mentions
        JOIN episodes ON episodes.id = raw_speaker_mentions.episode_id
        WHERE raw_speaker_mentions.resolution_status IN ('unresolved', 'candidate_match')
    """
    params: list[Any] = []
    if event_filter is not None:
        if not event_filter:
            return []
        sql += f" AND raw_speaker_mentions.discourse_event_id IN ({', '.join('?' for _ in event_filter)})"
        params.extend(event_filter)
    sql += " ORDER BY raw_speaker_mentions.created_at DESC, raw_speaker_mentions.id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params).fetchall()


def _actor_rows(conn, *, event_filter: list[str] | None, limit: int | None):
    sql = """
        SELECT raw_actor_mentions.*
        FROM raw_actor_mentions
        WHERE raw_actor_mentions.resolution_status IN ('unresolved', 'candidate_match')
    """
    params: list[Any] = []
    if event_filter is not None:
        if not event_filter:
            return []
        sql += f" AND raw_actor_mentions.discourse_event_id IN ({', '.join('?' for _ in event_filter)})"
        params.extend(event_filter)
    sql += " ORDER BY raw_actor_mentions.created_at DESC, raw_actor_mentions.id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params).fetchall()


def _upsert_entity(
    conn,
    *,
    entity_type: str,
    display_name: str,
    normalized_name: str,
    confidence: float,
    evidence: dict[str, Any],
    ts: str,
) -> str:
    table, prefix = ENTITY_TABLES[entity_type]
    entity_id = stable_id(entity_type, normalized_name, prefix=prefix)
    display_name = _canonical_display_name(display_name, normalized_name)
    conn.execute(
        f"""
        INSERT INTO {table}
          (id, display_name, normalized_name, confidence, status, evidence_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'candidate', ?, ?, ?)
        ON CONFLICT(normalized_name) DO UPDATE SET
          display_name = CASE
            WHEN {table}.display_name NOT LIKE '% %'
             AND excluded.display_name LIKE '% %'
            THEN excluded.display_name
            WHEN LENGTH({table}.display_name) > LENGTH(excluded.display_name)
             AND excluded.display_name != ''
            THEN excluded.display_name
            ELSE {table}.display_name
          END,
          confidence = MAX({table}.confidence, excluded.confidence),
          evidence_json = excluded.evidence_json,
          updated_at = excluded.updated_at
        """,
        (entity_id, display_name[:300], normalized_name, confidence, dumps_json(evidence), ts, ts),
    )
    existing = conn.execute(f"SELECT id FROM {table} WHERE normalized_name = ?", (normalized_name,)).fetchone()
    return existing["id"] if existing else entity_id


def _canonical_display_name(display_name: str, normalized_name: str) -> str:
    cleaned = re.sub(r"\s*/\s*(?:Latent Space|Big Technology|Practical AI|Dwarkesh Podcast|Microsoft Research Podcast)\b.*$", "", str(display_name or ""), flags=re.I).strip()
    cleaned = re.sub(r"\b(?:article page|show copy|page copy|editorial narrator|show notes author)\b", "", cleaned, flags=re.I).strip(" -_/")
    if cleaned:
        return cleaned[:300]
    return normalized_name.replace("_", " ").title()[:300]


def _upsert_alias(conn, *, entity_type: str, entity_id: str, alias: str, evidence: dict[str, Any], ts: str) -> None:
    normalized_alias = normalize_identity_name(alias)
    if not _usable_identity(normalized_alias):
        return
    conn.execute(
        """
        INSERT INTO identity_aliases
          (id, canonical_entity_type, canonical_entity_id, alias, normalized_alias, alias_type, status,
           canonical_version, evidence_count, evidence_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'surface_form', 'candidate', 1, 1, ?, ?, ?)
        ON CONFLICT(canonical_entity_type, canonical_entity_id, normalized_alias, canonical_version) DO UPDATE SET
          evidence_count = identity_aliases.evidence_count + 1,
          evidence_json = excluded.evidence_json,
          updated_at = excluded.updated_at
        """,
        (
            stable_id(entity_type, entity_id, normalized_alias, prefix="ialias_"),
            entity_type,
            entity_id,
            alias[:300],
            normalized_alias,
            dumps_json(evidence),
            ts,
            ts,
        ),
    )


def _ignore_speaker_row(conn, row) -> None:
    canonical_person_id = row["canonical_person_id"] if "canonical_person_id" in row.keys() else None
    if canonical_person_id:
        conn.execute(
            """
            DELETE FROM podcast_guest_edges
            WHERE episode_id = ?
              AND canonical_person_id = ?
            """,
            (row["episode_id"], canonical_person_id),
        )
    conn.execute(
        """
        UPDATE raw_speaker_mentions
        SET canonical_person_id = NULL,
            resolution_status = 'ignored_low_value'
        WHERE id = ?
        """,
        (row["id"],),
    )


def _ignore_actor_row(conn, row) -> None:
    conn.execute(
        """
        UPDATE raw_actor_mentions
        SET canonical_entity_id = NULL,
            resolution_status = 'ignored_low_value'
        WHERE id = ?
        """,
        (row["id"],),
    )


def _insert_resolution_candidate(
    conn,
    *,
    raw_table: str,
    raw_id: str,
    entity_type: str,
    entity_id: str,
    display_name: str,
    confidence: float,
    model: str,
    evidence: dict[str, Any],
    ts: str,
) -> None:
    status = "needs_human_review" if confidence < 0.7 else "candidate_match"
    conn.execute(
        """
        INSERT OR IGNORE INTO identity_resolution_candidates
          (id, raw_mention_table, raw_mention_id, candidate_entity_type, candidate_entity_id,
           candidate_display_name, status, confidence, judge_model, rationale, evidence_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'deterministic_bootstrap', ?, ?, ?, ?)
        """,
        (
            stable_id(raw_table, raw_id, entity_type, entity_id, prefix="irc_"),
            raw_table,
            raw_id,
            entity_type,
            entity_id,
            display_name[:300],
            status,
            confidence,
            f"Exact-normalized candidate only; pending {model} judge review before canonical promotion or merge.",
            dumps_json(evidence),
            ts,
            ts,
        ),
    )


def _is_guest_like(role: str | None) -> bool:
    normalized = str(role or "").strip().lower()
    return normalized and normalized not in {"host", "co-host", "cohost", "quoted_source"}


def _upsert_guest_edge(conn, *, row, canonical_person_id: str, ts: str) -> None:
    evidence = _speaker_evidence(row)
    conn.execute(
        """
        INSERT OR IGNORE INTO podcast_guest_edges
          (id, source_id, episode_id, canonical_person_id, role, confidence, evidence_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            stable_id(row["source_id"], row["episode_id"], canonical_person_id, str(row["role"] or "guest"), prefix="pge_"),
            row["source_id"],
            row["episode_id"],
            canonical_person_id,
            str(row["role"] or "guest")[:120],
            _confidence(row["confidence"], floor=0.6, cap=0.82),
            dumps_json(evidence),
            ts,
        ),
    )


def _build_person_concept_edges(conn, *, event_filter: list[str] | None, ts: str) -> Counter[str]:
    params: list[Any] = []
    sql = """
        SELECT
          raw_speaker_mentions.canonical_person_id,
          discourse_events.canonical_concept_id,
          discourse_events.candidate_concept,
          discourse_events.event_type,
          AVG(COALESCE(discourse_events.confidence, 0.6)) AS confidence,
          COUNT(*) AS weight,
          MIN(discourse_events.id) AS sample_event_id,
          MIN(discourse_events.label_id) AS sample_label_id,
          MIN(discourse_events.segment_id) AS sample_segment_id
        FROM raw_speaker_mentions
        JOIN discourse_events ON discourse_events.id = raw_speaker_mentions.discourse_event_id
        WHERE raw_speaker_mentions.canonical_person_id IS NOT NULL
    """
    if event_filter is not None:
        if not event_filter:
            return Counter()
        sql += f" AND discourse_events.id IN ({', '.join('?' for _ in event_filter)})"
        params.extend(event_filter)
    sql += """
        GROUP BY raw_speaker_mentions.canonical_person_id, discourse_events.canonical_concept_id,
                 discourse_events.candidate_concept, discourse_events.event_type
    """
    metrics: Counter[str] = Counter()
    for row in conn.execute(sql, params).fetchall():
        concept_name = row["candidate_concept"] or "unknown_concept"
        edge_type = row["event_type"] or "discourse_event"
        evidence = {
            "sample_discourse_event_id": row["sample_event_id"],
            "sample_label_id": row["sample_label_id"],
            "sample_segment_id": row["sample_segment_id"],
            "weight": row["weight"],
            "audit_status": "candidate_graph_edge_pending_gpt55_review",
        }
        conn.execute(
            """
            INSERT OR IGNORE INTO person_concept_edges
              (id, canonical_person_id, concept_id, concept_name, edge_type, weight, confidence, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_id(row["canonical_person_id"], row["canonical_concept_id"] or concept_name, edge_type, prefix="pce_"),
                row["canonical_person_id"],
                row["canonical_concept_id"],
                concept_name[:300],
                edge_type[:120],
                float(row["weight"] or 1),
                round(float(row["confidence"] or 0.6), 3),
                dumps_json(evidence),
                ts,
            ),
        )
        metrics["person_concept_edges_upserted"] += 1
    return metrics


def _build_person_person_mentions(
    conn,
    *,
    person_lookup: dict[str, str],
    event_filter: list[str] | None,
    ts: str,
) -> Counter[str]:
    params: list[Any] = []
    sql = """
        SELECT *
        FROM raw_actor_mentions
        WHERE canonical_entity_type = 'person'
          AND canonical_entity_id IS NOT NULL
          AND speaker_surface IS NOT NULL
    """
    if event_filter is not None:
        if not event_filter:
            return Counter()
        sql += f" AND discourse_event_id IN ({', '.join('?' for _ in event_filter)})"
        params.extend(event_filter)
    metrics: Counter[str] = Counter()
    for row in conn.execute(sql, params).fetchall():
        source_id = person_lookup.get(normalize_identity_name(row["speaker_surface"]))
        target_id = row["canonical_entity_id"]
        if not target_id or source_id == target_id:
            continue
        evidence = _actor_evidence(row)
        conn.execute(
            """
            INSERT OR IGNORE INTO person_person_mentions
              (id, source_person_id, target_person_id, source_surface, target_surface, episode_id, segment_id,
               discourse_event_id, confidence, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_id(row["discourse_event_id"], source_id or row["speaker_surface"], target_id, prefix="ppm_"),
                source_id,
                target_id,
                row["speaker_surface"],
                row["surface_name"],
                row["episode_id"],
                row["segment_id"],
                row["discourse_event_id"],
                _confidence(row["confidence"], floor=0.55, cap=0.8),
                dumps_json(evidence),
                ts,
            ),
        )
        metrics["person_person_mentions_upserted"] += 1
    return metrics


def _build_authority_scores(conn, *, graph_run_model: str, event_filter: list[str] | None, ts: str) -> str:
    score_run_id = stable_id("authority_scores", graph_run_model, ts, prefix="gsr_")
    conn.execute(
        """
        INSERT INTO graph_score_runs
          (id, run_type, model, status, parameters_json, metrics_json, created_at, updated_at)
        VALUES (?, 'authority_score_draft', ?, 'completed', ?, '{}', ?, ?)
        """,
        (
            score_run_id,
            "deterministic_bootstrap",
            dumps_json({"source": "v3_1_candidate_identity_graph", "event_scope": "pilot" if event_filter is not None else "all"}),
            ts,
            ts,
        ),
    )
    params: list[Any] = []
    sql = """
        SELECT
          person_concept_edges.canonical_person_id,
          person_concept_edges.concept_id,
          person_concept_edges.concept_name,
          SUM(person_concept_edges.weight) AS event_weight,
          AVG(person_concept_edges.confidence) AS avg_confidence,
          COUNT(DISTINCT podcast_guest_edges.source_id) AS podcast_reach,
          COUNT(DISTINCT podcast_guest_edges.episode_id) AS appearance_count
        FROM person_concept_edges
        LEFT JOIN podcast_guest_edges
          ON podcast_guest_edges.canonical_person_id = person_concept_edges.canonical_person_id
        WHERE 1=1
    """
    if event_filter is not None:
        # person_concept_edges are already scoped at insertion time for pilot runs.
        pass
    sql += """
        GROUP BY person_concept_edges.canonical_person_id, person_concept_edges.concept_id, person_concept_edges.concept_name
    """
    for row in conn.execute(sql, params).fetchall():
        event_weight = float(row["event_weight"] or 0)
        reach = float(row["podcast_reach"] or 0)
        appearances = float(row["appearance_count"] or 0)
        confidence = float(row["avg_confidence"] or 0.6)
        identity_penalty = 0.15 if confidence < 0.7 else 0.0
        score = round(max(0.0, event_weight + 0.75 * reach + 0.4 * appearances - identity_penalty), 3)
        components = {
            "appearance_count": appearances,
            "cross_podcast_reach": reach,
            "topical_claim_density": event_weight,
            "identity_confidence": round(confidence, 3),
            "identity_confidence_penalty": identity_penalty,
            "outcome_accuracy": None,
            "source_quality_weighting": None,
            "note": "preliminary deterministic score pending GPT-5.5 judge review and outcome checks",
        }
        conn.execute(
            """
            INSERT OR REPLACE INTO expert_authority_scores
              (id, canonical_person_id, concept_id, topic, score, status, components_json,
               graph_score_run_id, evidence_json, created_at)
            VALUES (?, ?, ?, ?, ?, 'preliminary', ?, ?, ?, ?)
            """,
            (
                stable_id(row["canonical_person_id"], row["concept_id"] or row["concept_name"] or "topic", score_run_id, prefix="eas_"),
                row["canonical_person_id"],
                row["concept_id"],
                row["concept_name"],
                score,
                dumps_json(components),
                score_run_id,
                dumps_json({"audit_status": "preliminary_pending_gpt55_judge", "graph_score_run_id": score_run_id}),
                ts,
            ),
        )
    return score_run_id


def _speaker_evidence(row) -> dict[str, Any]:
    return {
        "episode_id": row["episode_id"],
        "segment_id": row["segment_id"],
        "discourse_event_id": row["discourse_event_id"],
        "surface_name": row["surface_name"],
        "role": row["role"],
        "affiliation_surface": row["affiliation_surface"],
        "audit_status": "candidate_identity_pending_gpt55_judge",
    }


def _actor_evidence(row) -> dict[str, Any]:
    return {
        "episode_id": row["episode_id"],
        "segment_id": row["segment_id"],
        "discourse_event_id": row["discourse_event_id"],
        "surface_name": row["surface_name"],
        "mention_type": row["mention_type"],
        "speaker_surface": row["speaker_surface"],
        "reported_actor_surface": row["reported_actor_surface"],
        "why_matters": row["why_matters"],
        "audit_status": "candidate_identity_pending_gpt55_judge",
    }
