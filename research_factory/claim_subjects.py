from __future__ import annotations

import re
import sqlite3
from collections import Counter, defaultdict
from typing import Any

from .claim_canonicalizer import is_template_claim
from .util import dumps_json, loads_json, now_iso, stable_id


METHOD = "deterministic_technology_claim_subject_v1"
SUBJECT_RELATION = "about_subject"
CURRENT_LABEL_PACK = "ai_discourse_v3_1"
SUPPORTED_LABEL_PACKS = ("ai_discourse_v1", "ai_discourse_v2", "ai_discourse_v3", CURRENT_LABEL_PACK)
LEGACY_LABEL_PACKS = tuple(pack for pack in SUPPORTED_LABEL_PACKS if pack != CURRENT_LABEL_PACK)
EVENT_OBSERVATION_TYPES = {"frame_usage", "term_usage", "uncertainty"}
EVENT_ROLE_BY_TYPE = {
    "frame_usage": "framing",
    "term_usage": "terminology",
    "uncertainty": "uncertainty",
}

_ATTRIBUTION_RE = re.compile(
    r"^\s*(?:the\s+)?(?:host|guest|speaker|interviewer|interviewee|they|he|she|we|i|[A-Z][A-Za-z .'-]{1,80})\s+"
    r"(?:says|said|argues|argued|claims|claimed|believes|believed|thinks|thought|predicts|predicted|notes|noted|reports|reported|suggests|suggested)\s+"
    r"(?:that\s+)?",
)

_STOPWORDS = {
    "about",
    "after",
    "against",
    "also",
    "and",
    "are",
    "because",
    "before",
    "being",
    "between",
    "could",
    "from",
    "have",
    "into",
    "more",
    "most",
    "only",
    "over",
    "should",
    "than",
    "that",
    "their",
    "there",
    "these",
    "this",
    "through",
    "will",
    "with",
    "would",
}

_DOMAIN_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("software_platforms", ("database", "databases", "developer", "software", "platform", "saas", "open-source", "opensource")),
    ("hardware_chips", ("arm", "x86", "chip", "chips", "semiconductor", "gpu", "cpu", "server", "servers", "data center", "datacenter")),
    ("cybersecurity", ("cyber", "security", "breach", "breaches", "ransomware", "vulnerability", "liability")),
    ("robotics_transport", ("robotaxi", "robotaxis", "robot", "robots", "robotics", "autonomous vehicle", "drone", "drones")),
    ("energy_climate", ("fusion", "battery", "batteries", "grid", "solar", "geothermal", "climate", "energy")),
    ("biotech", ("drug", "drugs", "clinical", "trial", "trials", "biotech", "pharma", "discovery")),
    ("space_telecom", ("satellite", "broadband", "fiber", "telecom", "spectrum", "space", "launch")),
    ("cloud_infrastructure", ("cloud", "infrastructure", "kubernetes", "serverless", "compute", "storage")),
    ("markets_strategy", ("unit economics", "pricing", "margin", "margins", "profit", "profitable", "market", "share")),
    ("ai_ml", ("ai", "model", "models", "llm", "llms", "agent", "agents", "machine learning")),
]

_ENTITY_PATTERNS: list[tuple[str, str]] = [
    (r"\bopen[- ]source database(?:s)?\b", "open-source database"),
    (r"\bproprietary database(?:s)?\b", "proprietary database"),
    (r"\barm server(?:s)?\b", "ARM server"),
    (r"\bx86\b", "x86"),
    (r"\bcloud data ?center(?:s)?\b", "cloud data center"),
    (r"\bsoftware vendor(?:s)?\b", "software vendor"),
    (r"\bsecurity failure(?:s)?\b", "security failure"),
    (r"\brobotaxi(?:s)?\b", "robotaxi"),
    (r"\bdense urban market(?:s)?\b", "dense urban market"),
    (r"\bfusion\b", "fusion"),
    (r"\bgrid\b", "grid"),
    (r"\bai[- ]designed drug(?:s)?\b", "AI-designed drug"),
    (r"\bearly[- ]stage discovery\b", "early-stage discovery"),
    (r"\bclinical trial(?:s)?\b", "clinical trial"),
    (r"\bsatellite broadband\b", "satellite broadband"),
    (r"\brural fiber\b", "rural fiber"),
    (r"\bfrontier training cost(?:s)?\b", "frontier training cost"),
    (r"\bhumanoid robot(?:s)?\b", "humanoid robot"),
    (r"\bopen[- ]source model(?:s)?\b", "open-source model"),
    (r"\bproprietary model(?:s)?\b", "proprietary model"),
    (r"\bai coding agent(?:s)?\b", "AI coding agent"),
]

_SYNONYMS = {
    "opensource": "open-source",
    "open source": "open-source",
    "datacenters": "data center",
    "data centers": "data center",
    "profitability": "unit economics",
    "profitable": "unit economics",
    "economics": "unit economics",
    "commercialisation": "commercialization",
    "commercialize": "commercialization",
    "commercialise": "commercialization",
    "regulations": "regulation",
    "regulated": "regulation",
    "liabilities": "liability",
}


def build_claim_subjects(
    conn: sqlite3.Connection,
    *,
    pilot_id: str | None = None,
    scope: str = "last_18_months",
    model: str = "deterministic-local",
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    if scope not in {"last_18_months", "all"}:
        raise ValueError("scope must be last_18_months or all")
    started_at = now_iso()
    run_id = stable_id("claim_subject_run", started_at, scope, model, pilot_id or "all", prefix="csr_")
    params = {"pilot_id": pilot_id, "scope": scope, "limit": limit, "method": METHOD}
    rows = _claim_rows(conn, pilot_id=pilot_id, scope=scope, limit=limit)
    event_rows = _event_only_rows(conn, pilot_id=pilot_id, scope=scope, limit=limit)
    frames: list[dict[str, Any]] = []
    event_frames: list[dict[str, Any]] = []
    quarantined = 0
    quarantined_by_pack: Counter[str] = Counter()
    seen_by_pack = Counter(str(row.get("label_pack") or "unknown") for row in rows)
    framed_by_pack: Counter[str] = Counter()
    events_seen_by_type = Counter(str(row.get("event_type") or "unknown") for row in event_rows)
    events_framed_by_type: Counter[str] = Counter()
    events_quarantined_by_type: Counter[str] = Counter()
    for row in rows:
        if _should_quarantine_claim(row):
            quarantined += 1
            quarantined_by_pack[str(row.get("label_pack") or "unknown")] += 1
            continue
        frame = _frame_claim(row)
        frames.append(frame)
        framed_by_pack[str(frame.get("label_pack") or "unknown")] += 1
    for row in event_rows:
        if not _should_include_event_observation(row):
            events_quarantined_by_type[str(row.get("event_type") or "unknown")] += 1
            continue
        frame = _frame_event(row)
        event_frames.append(frame)
        events_framed_by_type[str(frame.get("event_type") or "unknown")] += 1

    subject_items = frames + event_frames
    subject_keys = {frame["subject_key"] for frame in subject_items}
    variant_keys = {(frame["subject_key"], frame["variant_key"]) for frame in frames}
    metrics = {
        "ok": True,
        "run_id": run_id,
        "scope": scope,
        "model": model,
        "method": METHOD,
        "claims_seen": len(rows),
        "claims_subject_framed": len(frames),
        "template_claims_quarantined": quarantined,
        "claims_seen_by_pack": dict(sorted(seen_by_pack.items())),
        "claims_subject_framed_by_pack": dict(sorted(framed_by_pack.items())),
        "claims_quarantined_by_pack": dict(sorted(quarantined_by_pack.items())),
        "legacy_claims_framed": sum(count for pack, count in framed_by_pack.items() if pack in LEGACY_LABEL_PACKS),
        "event_observations_seen": len(event_rows),
        "event_observations_framed": len(event_frames),
        "event_observations_quarantined": sum(events_quarantined_by_type.values()),
        "event_observations_seen_by_type": dict(sorted(events_seen_by_type.items())),
        "event_observations_framed_by_type": dict(sorted(events_framed_by_type.items())),
        "event_observations_quarantined_by_type": dict(sorted(events_quarantined_by_type.items())),
        "subjects_considered": len(subject_keys),
        "variants_considered": len(variant_keys),
        "observations_considered": len(frames) + len(event_frames),
        "dry_run": dry_run,
    }
    if dry_run:
        return metrics

    conn.execute(
        """
        INSERT INTO claim_subject_runs
          (id, scope, model, status, parameters_json, metrics_json, started_at, completed_at, created_at, updated_at)
        VALUES (?, ?, ?, 'running', ?, '{}', ?, NULL, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          status = 'running',
          parameters_json = excluded.parameters_json,
          started_at = excluded.started_at,
          updated_at = excluded.updated_at
        """,
        (run_id, scope, model, dumps_json(params), started_at, started_at, started_at),
    )
    try:
        ts = now_iso()
        _delete_existing_for_claims(conn, [str(row["claim_id"]) for row in rows])
        _delete_existing_for_events(conn, [str(row["discourse_event_id"]) for row in event_rows])
        subject_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        variant_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for frame in subject_items:
            subject_groups[frame["subject_key"]].append(frame)
        for frame in frames:
            variant_groups[(frame["subject_key"], frame["variant_key"])].append(frame)

        subject_ids: dict[str, str] = {}
        variant_ids: dict[tuple[str, str], str] = {}
        for subject_key, items in subject_groups.items():
            subject_id = stable_id("claim_subject", METHOD, subject_key, prefix="csj_")
            subject_ids[subject_key] = subject_id
            evidence = _subject_evidence(items)
            conn.execute(
                """
                INSERT INTO claim_subjects
                  (id, subject_text, subject_key, domain, subject_type, status, confidence, method, evidence_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'technology_issue', ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subject_key, method) DO UPDATE SET
                  subject_text = excluded.subject_text,
                  domain = excluded.domain,
                  status = excluded.status,
                  confidence = excluded.confidence,
                  evidence_json = excluded.evidence_json,
                  updated_at = excluded.updated_at
                """,
                (
                    subject_id,
                    _best_text(items, "subject_text"),
                    subject_key,
                    _majority(items, "domain"),
                    _status_for_subject(items),
                    _confidence(items, base=0.68),
                    METHOD,
                    dumps_json(evidence),
                    ts,
                    ts,
                ),
            )
            for item in items:
                if not item.get("claim_id"):
                    continue
                conn.execute(
                    """
                    INSERT INTO claim_subject_members
                      (id, subject_id, claim_id, relation, confidence, method, run_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(subject_id, claim_id, relation) DO UPDATE SET
                      confidence = excluded.confidence,
                      method = excluded.method,
                      run_id = excluded.run_id,
                      updated_at = excluded.updated_at
                    """,
                    (
                        stable_id("claim_subject_member", subject_id, item["claim_id"], SUBJECT_RELATION, prefix="csm_"),
                        subject_id,
                        item["claim_id"],
                        SUBJECT_RELATION,
                        item["confidence"],
                        METHOD,
                        run_id,
                        ts,
                        ts,
                    ),
                )
                metrics["memberships_upserted"] = metrics.get("memberships_upserted", 0) + 1
            metrics["subjects_upserted"] = len(subject_groups)

        for key, items in variant_groups.items():
            subject_key, variant_key = key
            subject_id = subject_ids[subject_key]
            variant_id = stable_id("claim_variant", METHOD, subject_id, variant_key, prefix="cpv_")
            variant_ids[key] = variant_id
            sample = items[0]
            conn.execute(
                """
                INSERT INTO claim_proposition_variants
                  (id, subject_id, variant_text, variant_key, predicate, object_text, technology_system, market_context,
                   geography, horizon, numeric_value, numeric_unit, condition_text, polarity, status, confidence, method,
                   evidence_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'candidate', ?, ?, ?, ?, ?)
                ON CONFLICT(subject_id, variant_key, method) DO UPDATE SET
                  variant_text = excluded.variant_text,
                  predicate = excluded.predicate,
                  object_text = excluded.object_text,
                  technology_system = excluded.technology_system,
                  market_context = excluded.market_context,
                  geography = excluded.geography,
                  horizon = excluded.horizon,
                  numeric_value = excluded.numeric_value,
                  numeric_unit = excluded.numeric_unit,
                  condition_text = excluded.condition_text,
                  polarity = excluded.polarity,
                  confidence = excluded.confidence,
                  evidence_json = excluded.evidence_json,
                  updated_at = excluded.updated_at
                """,
                (
                    variant_id,
                    subject_id,
                    _best_text(items, "variant_text"),
                    variant_key,
                    sample.get("predicate"),
                    sample.get("object_text"),
                    sample.get("technology_system"),
                    sample.get("market_context"),
                    sample.get("geography"),
                    sample.get("horizon"),
                    sample.get("numeric_value"),
                    sample.get("numeric_unit"),
                    sample.get("condition_text"),
                    sample.get("polarity"),
                    _confidence(items, base=0.64),
                    METHOD,
                    dumps_json(_variant_evidence(items)),
                    ts,
                    ts,
                ),
            )
            metrics["variants_upserted"] = metrics.get("variants_upserted", 0) + 1

        for item in frames:
            subject_id = subject_ids[item["subject_key"]]
            variant_id = variant_ids[(item["subject_key"], item["variant_key"])]
            conn.execute(
                """
                INSERT INTO claim_position_observations
                  (id, subject_id, variant_id, claim_id, discourse_event_id, canonical_person_id, speaker_name,
                   speaker_affiliation, source_id, episode_id, stance, certainty, frame, published_at, evidence_json,
                   confidence, method, run_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(claim_id, subject_id, variant_id) DO UPDATE SET
                  discourse_event_id = excluded.discourse_event_id,
                  canonical_person_id = excluded.canonical_person_id,
                  speaker_name = excluded.speaker_name,
                  speaker_affiliation = excluded.speaker_affiliation,
                  source_id = excluded.source_id,
                  episode_id = excluded.episode_id,
                  stance = excluded.stance,
                  certainty = excluded.certainty,
                  frame = excluded.frame,
                  published_at = excluded.published_at,
                  evidence_json = excluded.evidence_json,
                  confidence = excluded.confidence,
                  method = excluded.method,
                  run_id = excluded.run_id,
                  updated_at = excluded.updated_at
                """,
                (
                    stable_id("claim_position_observation", item["claim_id"], subject_id, variant_id, prefix="cpo_"),
                    subject_id,
                    variant_id,
                    item["claim_id"],
                    item.get("discourse_event_id"),
                    item.get("canonical_person_id"),
                    item.get("speaker_name"),
                    item.get("speaker_affiliation"),
                    item.get("source_id"),
                    item.get("episode_id"),
                    item.get("stance"),
                    item.get("certainty"),
                    item.get("frame"),
                    item.get("published_at"),
                    dumps_json(_observation_evidence(item)),
                    item["confidence"],
                    METHOD,
                    run_id,
                    ts,
                    ts,
                ),
            )
            metrics["observations_upserted"] = metrics.get("observations_upserted", 0) + 1

        for item in event_frames:
            subject_id = subject_ids[item["subject_key"]]
            conn.execute(
                """
                INSERT INTO claim_subject_event_observations
                  (id, subject_id, discourse_event_id, event_type, event_role, canonical_person_id, speaker_name,
                   speaker_affiliation, source_id, episode_id, stance, certainty, frame, concept_name, published_at,
                   evidence_json, confidence, method, run_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(discourse_event_id, subject_id, method) DO UPDATE SET
                  event_type = excluded.event_type,
                  event_role = excluded.event_role,
                  canonical_person_id = excluded.canonical_person_id,
                  speaker_name = excluded.speaker_name,
                  speaker_affiliation = excluded.speaker_affiliation,
                  source_id = excluded.source_id,
                  episode_id = excluded.episode_id,
                  stance = excluded.stance,
                  certainty = excluded.certainty,
                  frame = excluded.frame,
                  concept_name = excluded.concept_name,
                  published_at = excluded.published_at,
                  evidence_json = excluded.evidence_json,
                  confidence = excluded.confidence,
                  run_id = excluded.run_id,
                  updated_at = excluded.updated_at
                """,
                (
                    stable_id("claim_subject_event_observation", item["discourse_event_id"], subject_id, prefix="cseo_"),
                    subject_id,
                    item["discourse_event_id"],
                    item["event_type"],
                    item["event_role"],
                    item.get("canonical_person_id"),
                    item.get("speaker_name"),
                    item.get("speaker_affiliation"),
                    item.get("source_id"),
                    item.get("episode_id"),
                    item.get("stance"),
                    item.get("certainty"),
                    item.get("frame"),
                    item.get("concept_name"),
                    item.get("published_at"),
                    dumps_json(_event_observation_evidence(item)),
                    item["confidence"],
                    METHOD,
                    run_id,
                    ts,
                    ts,
                ),
            )
            metrics["event_observations_upserted"] = metrics.get("event_observations_upserted", 0) + 1

        metrics["expert_positions_upserted"] = _rebuild_expert_positions(conn, run_id=run_id, ts=ts)
        _cleanup_unreferenced_subjects(conn)

        completed_at = now_iso()
        conn.execute(
            """
            UPDATE claim_subject_runs
            SET status = 'completed', metrics_json = ?, completed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (dumps_json(metrics), completed_at, completed_at, run_id),
        )
        conn.commit()
    except Exception:
        failed_at = now_iso()
        conn.execute(
            "UPDATE claim_subject_runs SET status = 'failed', metrics_json = ?, completed_at = ?, updated_at = ? WHERE id = ?",
            (dumps_json({**metrics, "ok": False}), failed_at, failed_at, run_id),
        )
        conn.commit()
        raise
    return metrics


def _claim_rows(conn: sqlite3.Connection, *, pilot_id: str | None, scope: str, limit: int | None) -> list[dict[str, Any]]:
    max_day = conn.execute(
        """
        SELECT MAX(substr(episodes.published_at, 1, 10)) AS day
        FROM claims
        JOIN segments ON segments.id = claims.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        WHERE episodes.published_at IS NOT NULL
        """
    ).fetchone()["day"]
    sql = """
        WITH matched_events AS (
          SELECT
            discourse_events.id AS discourse_event_id,
            discourse_events.label_id,
            discourse_events.segment_id,
            discourse_events.claim_text,
            MIN(discourse_events.event_type) AS event_type,
            MIN(discourse_events.actor_name) AS actor_name,
            MIN(discourse_events.actor_affiliation) AS actor_affiliation,
            MIN(discourse_events.canonical_concept_id) AS concept_id,
            MIN(COALESCE(NULLIF(discourse_events.canonical_concept_name, ''), NULLIF(discourse_events.candidate_concept, ''))) AS concept_name,
            MIN(discourse_events.certainty) AS certainty,
            MIN(discourse_events.temporal_horizon) AS temporal_horizon,
            MIN(discourse_events.frame) AS frame,
            MIN(discourse_events.organizations_json) AS organizations_json,
            MIN(discourse_events.product_names_json) AS product_names_json,
            MIN(discourse_events.model_names_json) AS model_names_json,
            MIN(discourse_events.people_json) AS people_json
          FROM discourse_events
          WHERE discourse_events.claim_text IS NOT NULL AND discourse_events.claim_text != ''
          GROUP BY discourse_events.label_id, discourse_events.segment_id, discourse_events.claim_text
        ),
        speaker_mentions AS (
          SELECT
            raw_speaker_mentions.discourse_event_id,
            MIN(raw_speaker_mentions.canonical_person_id) AS canonical_person_id,
            MIN(raw_speaker_mentions.surface_name) AS surface_name
          FROM raw_speaker_mentions
          GROUP BY raw_speaker_mentions.discourse_event_id
        )
        SELECT
          claims.id AS claim_id,
          claims.label_id,
          claims.segment_id,
          claims.text,
          claims.stance,
          claims.confidence,
          labels.label_pack,
          labels.status AS label_status,
          segments.episode_id,
          episodes.published_at,
          sources.id AS source_id,
          sources.name AS source_name,
          matched_events.discourse_event_id,
          matched_events.event_type,
          matched_events.actor_name,
          matched_events.actor_affiliation,
          matched_events.concept_id,
          matched_events.concept_name,
          matched_events.certainty,
          matched_events.temporal_horizon,
          matched_events.frame,
          matched_events.organizations_json,
          matched_events.product_names_json,
          matched_events.model_names_json,
          matched_events.people_json,
          speaker_mentions.canonical_person_id,
          speaker_mentions.surface_name
        FROM claims
        JOIN labels ON labels.id = claims.label_id
        JOIN segments ON segments.id = claims.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        LEFT JOIN matched_events
          ON matched_events.label_id = claims.label_id
         AND matched_events.segment_id = claims.segment_id
         AND matched_events.claim_text = claims.text
        LEFT JOIN speaker_mentions
          ON speaker_mentions.discourse_event_id = matched_events.discourse_event_id
        WHERE labels.label_pack IN ('ai_discourse_v1', 'ai_discourse_v2', 'ai_discourse_v3', 'ai_discourse_v3_1')
    """
    params: list[Any] = []
    if scope == "last_18_months" and max_day:
        sql += " AND episodes.published_at IS NOT NULL AND date(substr(episodes.published_at, 1, 10)) >= date(?, '-18 month')"
        params.append(max_day)
    if pilot_id:
        sql += """
          AND EXISTS (
            SELECT 1
            FROM jobs
            WHERE jobs.target_id = claims.segment_id
              AND jobs.job_type = 'label_segment'
              AND json_extract(jobs.payload_json, '$.pilot_id') = ?
          )
        """
        params.append(pilot_id)
    sql += " ORDER BY episodes.published_at DESC, claims.created_at DESC, claims.id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _event_only_rows(conn: sqlite3.Connection, *, pilot_id: str | None, scope: str, limit: int | None) -> list[dict[str, Any]]:
    max_day = conn.execute(
        """
        SELECT MAX(substr(episodes.published_at, 1, 10)) AS day
        FROM discourse_events
        JOIN labels ON labels.id = discourse_events.label_id
        JOIN segments ON segments.id = discourse_events.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        WHERE labels.label_pack = ?
          AND episodes.published_at IS NOT NULL
        """,
        (CURRENT_LABEL_PACK,),
    ).fetchone()["day"]
    sql = """
        WITH speaker_mentions AS (
          SELECT
            raw_speaker_mentions.discourse_event_id,
            MIN(raw_speaker_mentions.canonical_person_id) AS canonical_person_id,
            MIN(raw_speaker_mentions.surface_name) AS surface_name
          FROM raw_speaker_mentions
          GROUP BY raw_speaker_mentions.discourse_event_id
        )
        SELECT
          discourse_events.id AS discourse_event_id,
          discourse_events.label_id,
          discourse_events.segment_id,
          discourse_events.event_type,
          discourse_events.actor_name,
          discourse_events.actor_affiliation,
          discourse_events.canonical_concept_id AS concept_id,
          COALESCE(NULLIF(discourse_events.canonical_concept_name, ''), NULLIF(discourse_events.candidate_concept, '')) AS concept_name,
          discourse_events.stance,
          discourse_events.claim_text AS text,
          discourse_events.certainty,
          discourse_events.temporal_horizon,
          discourse_events.frame,
          discourse_events.confidence,
          discourse_events.surface_terms_json,
          discourse_events.organizations_json,
          discourse_events.product_names_json,
          discourse_events.model_names_json,
          discourse_events.people_json,
          labels.label_pack,
          labels.status AS label_status,
          segments.episode_id,
          episodes.published_at,
          sources.id AS source_id,
          sources.name AS source_name,
          speaker_mentions.canonical_person_id,
          speaker_mentions.surface_name
        FROM discourse_events
        JOIN labels ON labels.id = discourse_events.label_id
        JOIN segments ON segments.id = discourse_events.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        LEFT JOIN claims
          ON claims.label_id = discourse_events.label_id
         AND claims.segment_id = discourse_events.segment_id
         AND claims.text = discourse_events.claim_text
        LEFT JOIN speaker_mentions
          ON speaker_mentions.discourse_event_id = discourse_events.id
        WHERE labels.label_pack = ?
          AND discourse_events.claim_text IS NOT NULL
          AND discourse_events.claim_text != ''
          AND claims.id IS NULL
    """
    params: list[Any] = [CURRENT_LABEL_PACK]
    if scope == "last_18_months" and max_day:
        sql += " AND episodes.published_at IS NOT NULL AND date(substr(episodes.published_at, 1, 10)) >= date(?, '-18 month')"
        params.append(max_day)
    if pilot_id:
        sql += """
          AND EXISTS (
            SELECT 1
            FROM jobs
            WHERE jobs.target_id = discourse_events.segment_id
              AND jobs.job_type = 'label_segment'
              AND json_extract(jobs.payload_json, '$.pilot_id') = ?
          )
        """
        params.append(pilot_id)
    sql += " ORDER BY episodes.published_at DESC, discourse_events.created_at DESC, discourse_events.id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _delete_existing_for_claims(conn: sqlite3.Connection, claim_ids: list[str]) -> None:
    if not claim_ids:
        return
    for index in range(0, len(claim_ids), 500):
        chunk = claim_ids[index : index + 500]
        placeholders = ",".join("?" for _ in chunk)
        conn.execute(
            f"DELETE FROM claim_position_observations WHERE method = ? AND claim_id IN ({placeholders})",
            (METHOD, *chunk),
        )
        conn.execute(
            f"DELETE FROM claim_subject_members WHERE method = ? AND claim_id IN ({placeholders})",
            (METHOD, *chunk),
        )
    _cleanup_unreferenced_subjects(conn)


def _delete_existing_for_events(conn: sqlite3.Connection, discourse_event_ids: list[str]) -> None:
    if not discourse_event_ids:
        return
    for index in range(0, len(discourse_event_ids), 500):
        chunk = discourse_event_ids[index : index + 500]
        placeholders = ",".join("?" for _ in chunk)
        conn.execute(
            f"DELETE FROM claim_subject_event_observations WHERE method = ? AND discourse_event_id IN ({placeholders})",
            (METHOD, *chunk),
        )
    _cleanup_unreferenced_subjects(conn)


def _cleanup_unreferenced_subjects(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        DELETE FROM claim_subject_expert_positions
        WHERE method = ?
          AND subject_id NOT IN (SELECT DISTINCT subject_id FROM claim_position_observations WHERE subject_id IS NOT NULL)
          AND subject_id NOT IN (SELECT DISTINCT subject_id FROM claim_subject_event_observations WHERE subject_id IS NOT NULL)
        """,
        (METHOD,),
    )
    conn.execute(
        """
        DELETE FROM claim_proposition_variants
        WHERE method = ?
          AND id NOT IN (SELECT DISTINCT variant_id FROM claim_position_observations WHERE variant_id IS NOT NULL)
        """,
        (METHOD,),
    )
    conn.execute(
        """
        DELETE FROM claim_subjects
        WHERE method = ?
          AND id NOT IN (SELECT DISTINCT subject_id FROM claim_subject_members WHERE subject_id IS NOT NULL)
          AND id NOT IN (SELECT DISTINCT subject_id FROM claim_position_observations WHERE subject_id IS NOT NULL)
          AND id NOT IN (SELECT DISTINCT subject_id FROM claim_subject_event_observations WHERE subject_id IS NOT NULL)
        """,
        (METHOD,),
    )


def _frame_claim(row: dict[str, Any]) -> dict[str, Any]:
    text = str(row.get("text") or "").strip()
    clean = _clean_text(text)
    domain = _detect_domain(clean, row)
    entities = _entities(clean, row)
    issue = _issue_type(clean)
    issue_from_frame = issue == "strategic outlook"
    if issue_from_frame:
        issue = _fallback_issue(clean, row)
    subject_base = _subject_base(clean, row, entities, issue, issue_from_frame=issue_from_frame)
    subject_text = _subject_text(subject_base, issue)
    horizon = _horizon(clean, row)
    market_context = _market_context(clean)
    geography = _geography(clean)
    numeric_value, numeric_unit = _numeric(clean)
    condition_text = _condition(clean)
    polarity = "negative" if _has_negation(clean) else "positive"
    predicate = _predicate(issue, clean)
    object_text = (
        _fallback_variant_object(entities, clean, row, issue)
        if issue_from_frame
        else _object_text(entities, clean, subject_base)
    )
    variant_text = _variant_text(text)
    subject_key = "|".join([_key(subject_base), _key(issue)])
    variant_parts = [subject_key, _key(predicate), _key(object_text), _key(horizon), _key(market_context), _key(geography), _key(condition_text), polarity, _key(numeric_value), _key(numeric_unit)]
    variant_key = "|".join(part for part in variant_parts if part)
    label_pack = str(row.get("label_pack") or "")
    confidence = _claim_confidence(row, bool(entities))
    return {
        "claim_id": row["claim_id"],
        "discourse_event_id": row.get("discourse_event_id"),
        "subject_text": subject_text,
        "subject_key": subject_key,
        "variant_text": variant_text,
        "variant_key": variant_key,
        "domain": domain,
        "predicate": predicate,
        "object_text": object_text,
        "technology_system": subject_base,
        "market_context": market_context,
        "geography": geography,
        "horizon": horizon,
        "numeric_value": numeric_value,
        "numeric_unit": numeric_unit,
        "condition_text": condition_text,
        "polarity": polarity,
        "speaker_name": row.get("surface_name") or row.get("actor_name"),
        "speaker_affiliation": row.get("actor_affiliation"),
        "canonical_person_id": row.get("canonical_person_id"),
        "source_id": row.get("source_id"),
        "source_name": row.get("source_name"),
        "episode_id": row.get("episode_id"),
        "published_at": row.get("published_at"),
        "stance": row.get("stance"),
        "certainty": row.get("certainty"),
        "frame": row.get("frame") or issue,
        "event_type": row.get("event_type"),
        "concept_name": row.get("concept_name"),
        "confidence": confidence,
        "claim_text": text[:1000],
        "label_pack": label_pack,
        "label_status": row.get("label_status"),
        "legacy_adapter": label_pack in LEGACY_LABEL_PACKS,
    }


def _frame_event(row: dict[str, Any]) -> dict[str, Any]:
    event_type = str(row.get("event_type") or "event").strip()
    event_role = EVENT_ROLE_BY_TYPE.get(event_type, _phrase_from_identifier(event_type) or "event")
    concept = _phrase_from_identifier(row.get("concept_name")) or _phrase_from_identifier(row.get("frame"))
    surface_terms = loads_json(row.get("surface_terms_json"), default=[]) or []
    if not concept and surface_terms:
        concept = _phrase_from_identifier(surface_terms[0])
    text_parts = [
        concept or "",
        _phrase_from_identifier(row.get("frame")) or "",
        " ".join(str(item) for item in surface_terms[:3]),
        str(row.get("text") or ""),
    ]
    clean = _clean_text(" ".join(part for part in text_parts if part))
    entities = _entities(clean, row)
    subject_base = concept or (" and ".join(entities[:2]) if entities else "technology discourse")
    subject_text = _subject_text(subject_base, event_role)
    subject_key = "|".join([_key(subject_base), _key(event_role)])
    confidence = _event_confidence(row, bool(entities or concept))
    return {
        "discourse_event_id": row["discourse_event_id"],
        "subject_text": subject_text,
        "subject_key": subject_key,
        "domain": _detect_domain(clean, row),
        "event_type": event_type,
        "event_role": event_role,
        "speaker_name": row.get("surface_name") or row.get("actor_name"),
        "speaker_affiliation": row.get("actor_affiliation"),
        "canonical_person_id": row.get("canonical_person_id"),
        "source_id": row.get("source_id"),
        "source_name": row.get("source_name"),
        "episode_id": row.get("episode_id"),
        "published_at": row.get("published_at"),
        "stance": row.get("stance"),
        "certainty": row.get("certainty"),
        "frame": row.get("frame") or event_role,
        "concept_name": row.get("concept_name"),
        "confidence": confidence,
        "label_pack": row.get("label_pack"),
        "label_status": row.get("label_status"),
    }


def _clean_text(text: str) -> str:
    value = _ATTRIBUTION_RE.sub("", text.strip()).lower()
    value = value.replace("open source", "open-source").replace("closed source", "closed-source")
    for source, target in _SYNONYMS.items():
        value = value.replace(source, target)
    return " ".join(value.split())


def _detect_domain(text: str, row: dict[str, Any]) -> str:
    concept = str(row.get("concept_name") or "").lower()
    haystack = f"{text} {concept}"
    for domain, keywords in _DOMAIN_KEYWORDS:
        if any(keyword in haystack for keyword in keywords):
            return domain
    return "technology_general"


def _entities(text: str, row: dict[str, Any]) -> list[str]:
    found: list[str] = []
    for pattern, label in _ENTITY_PATTERNS:
        if re.search(pattern, text, re.I):
            found.append(label)
    for field in ("organizations_json", "product_names_json", "model_names_json"):
        for value in loads_json(row.get(field), default=[]) or []:
            label = str(value or "").strip()
            if (
                label
                and _entity_mentioned(label, text)
                and label.lower() not in {item.lower() for item in found}
            ):
                found.append(label[:80])
    if not found and row.get("concept_name"):
        found.append(str(row["concept_name"]).replace("_", " ")[:80])
    return found[:4]


def _issue_type(text: str) -> str:
    if any(term in text for term in ("liability", "regulation", "regulated", "law", "policy")):
        return "liability regulation" if "liability" in text else "regulation"
    if any(term in text for term in ("unit economics", "profit", "profitable", "margin", "pricing", "cost", "economics")):
        return "unit economics"
    if any(term in text for term in ("market share", "taking share", "take share", "gain share", "adoption", "adopted", "deploy", "deployment")):
        return "market adoption"
    if (
        "timeline" in text
        or "commercialization" in text
        or "grid-relevant" in text
        or re.search(r"\b20[2-9][0-9]s?\b", text)
        or re.search(r"\b(?:within|in|before|after|by)\s+(?:the\s+)?(?:next\s+)?(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|a|an)\s+(?:year|years|month|months|decade|decades)\b", text)
    ):
        return "timeline"
    if any(term in text for term in ("productive", "productivity", "shorten", "improve", "faster", "automate")):
        return "productivity impact"
    if any(term in text for term in ("closing the gap", "compete", "competitive", "outperform", "catch up")):
        return "competitiveness"
    if any(term in text for term in ("risk", "breach", "security", "safety", "failure")):
        return "risk"
    return "strategic outlook"


def _fallback_issue(text: str, row: dict[str, Any]) -> str:
    frame = _phrase_from_identifier(row.get("frame"))
    if frame and _key(frame) not in {"forecast", "prediction", "claim", "analysis", "strategic_outlook"}:
        return frame
    concept = _phrase_from_identifier(row.get("concept_name"))
    if concept:
        return concept
    tokens = _tokens(text)
    return " ".join(tokens[:4]) or "strategic outlook"


def _subject_base(
    text: str,
    row: dict[str, Any],
    entities: list[str],
    issue: str,
    *,
    issue_from_frame: bool = False,
) -> str:
    if issue_from_frame:
        return issue
    if "open-source database" in entities:
        return "open-source database"
    if "ARM server" in entities:
        return "ARM server"
    if "software vendor" in entities and ("security" in text or "breach" in text):
        return "software vendor cybersecurity"
    if "robotaxi" in entities:
        return "robotaxi"
    if "fusion" in entities:
        return "fusion commercialization"
    if "AI-designed drug" in entities:
        return "AI-designed drug discovery"
    if "satellite broadband" in entities:
        return "satellite broadband and rural fiber"
    if "frontier training cost" in entities:
        return "frontier training cost"
    if "humanoid robot" in entities:
        return "humanoid robot deployment"
    if entities:
        return " and ".join(entities[:2])
    concept = _phrase_from_identifier(row.get("concept_name"))
    if concept and _key(concept) != _key(issue):
        return concept
    tokens = _tokens(text)
    return " ".join(tokens[:4]) or "technology claim"


def _subject_text(subject_base: str, issue: str) -> str:
    if _key(subject_base) == _key(issue):
        text = subject_base
    else:
        text = f"{subject_base} {issue}".strip()
    return " ".join(word.upper() if word in {"ARM", "AI"} else word for word in _title_words(text))


def _title_words(text: str) -> list[str]:
    words: list[str] = []
    for word in text.split():
        if word.lower() in {"and", "or", "for", "of", "in", "to", "x86"}:
            words.append(word)
        elif word.upper() in {"AI", "ARM"}:
            words.append(word.upper())
        else:
            words.append(word[:1].upper() + word[1:])
    return words


def _horizon(text: str, row: dict[str, Any]) -> str | None:
    if match := re.search(r"\b(20[2-9][0-9]s?)\b", text):
        return match.group(1)
    if match := re.search(r"\b(?:within|in|before|after|by)\s+([a-z0-9 -]{1,24}?(?:year|years|month|months|decade|2030s?))\b", text):
        return match.group(1).strip()
    horizon = str(row.get("temporal_horizon") or "").strip()
    return horizon or None


def _market_context(text: str) -> str | None:
    for phrase in ("cloud data center", "dense urban market", "rural fiber", "factories", "homes", "clinical trials", "early-stage discovery"):
        if phrase in text:
            return phrase
    return None


def _geography(text: str) -> str | None:
    for phrase in ("us", "u.s.", "europe", "eu", "china", "rural", "urban"):
        if re.search(rf"\b{re.escape(phrase)}\b", text):
            return "US" if phrase in {"us", "u.s."} else phrase
    return None


def _numeric(text: str) -> tuple[str | None, str | None]:
    if match := re.search(r"\b(\d+(?:\.\d+)?)\s*(%|percent|x|million|billion|years?|months?)\b", text):
        return match.group(1), match.group(2)
    return None, None


def _condition(text: str) -> str | None:
    if match := re.search(r"\b(?:if|unless|only in|only when|provided that)\s+([^.;,]{3,80})", text):
        return match.group(1).strip()
    if "dense urban market" in text:
        return "dense urban markets"
    return None


def _has_negation(text: str) -> bool:
    return bool(re.search(r"\b(no|not|never|unlikely|cannot|can't|won't|will not|isn't|aren't)\b", text))


def _predicate(issue: str, text: str) -> str:
    if issue == "timeline":
        return "arrival_or_commercialization_timeline"
    if issue == "unit economics":
        return "economic_viability"
    if issue == "market adoption":
        return "market_adoption_or_share"
    if issue == "liability regulation":
        return "regulatory_liability"
    if issue == "productivity impact":
        return "productivity_or_process_impact"
    if issue == "competitiveness":
        return "competitive_position"
    if issue == "risk":
        return "risk_or_failure"
    return _key(" ".join(_tokens(text)[:4])) or "strategic_outlook"


def _object_text(entities: list[str], text: str, subject_base: str) -> str:
    if subject_base:
        return subject_base
    if len(entities) >= 2:
        return " / ".join(entities[:3])
    if entities:
        return entities[0]
    return " ".join(_tokens(text)[:6])


def _fallback_variant_object(entities: list[str], text: str, row: dict[str, Any], issue: str) -> str:
    if entities:
        return " / ".join(entities[:3])
    concept = _phrase_from_identifier(row.get("concept_name"))
    if concept and _key(concept) != _key(issue):
        return concept
    return " ".join(_tokens(text)[:8])


def _variant_text(text: str) -> str:
    stripped = _ATTRIBUTION_RE.sub("", text.strip())
    return stripped[:1000]


def _tokens(text: str) -> list[str]:
    value = re.sub(r"[^a-zA-Z0-9-]+", " ", text.lower())
    tokens: list[str] = []
    for token in value.split():
        if len(token) < 3 or token in _STOPWORDS:
            continue
        if token.endswith("ies") and len(token) > 5:
            token = token[:-3] + "y"
        elif token.endswith("s") and len(token) > 4 and not token.endswith("ss"):
            token = token[:-1]
        if token not in _STOPWORDS:
            tokens.append(token)
    return tokens


def _phrase_from_identifier(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"[^a-zA-Z0-9 .]+", " ", text)
    words = [word.lower() for word in text.split() if len(word) >= 2]
    if not words:
        return None
    return " ".join(words[:6])


def _entity_mentioned(label: str, text: str) -> bool:
    label_key = _key(label)
    if not label_key:
        return False
    text_key = _key(text)
    if label_key in text_key:
        return True
    label_tokens = [token for token in _tokens(label) if token not in {"inc", "corp", "company"}]
    text_tokens = set(_tokens(text))
    return bool(label_tokens) and all(token in text_tokens for token in label_tokens[:3])


def _key(value: Any) -> str:
    text = str(value or "").lower()
    text = text.replace("open source", "open-source")
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text[:96]


def _best_text(items: list[dict[str, Any]], key: str) -> str:
    candidates = [str(item.get(key) or "").strip() for item in items if item.get(key)]
    candidates.sort(key=lambda value: (len(value), value.lower()))
    return candidates[0][:1000] if candidates else ""


def _majority(items: list[dict[str, Any]], key: str) -> str | None:
    counts = Counter(str(item.get(key) or "") for item in items if item.get(key))
    if not counts:
        return None
    return counts.most_common(1)[0][0]


def _confidence(items: list[dict[str, Any]], *, base: float) -> float:
    source_count = len({item.get("source_id") for item in items if item.get("source_id")})
    episode_count = len({item.get("episode_id") for item in items if item.get("episode_id")})
    return round(min(0.92, base + min(0.1, source_count * 0.025) + min(0.08, episode_count * 0.015)), 3)


def _claim_confidence(row: dict[str, Any], has_entities: bool) -> float:
    confidence = float(row.get("confidence") or 0.65)
    if has_entities:
        confidence += 0.05
    if str(row.get("label_pack") or "") in LEGACY_LABEL_PACKS:
        confidence -= 0.12
    if str(row.get("label_status") or "").startswith("quarantined"):
        confidence -= 0.18
    return round(min(0.9, max(0.35, confidence)), 3)


def _event_confidence(row: dict[str, Any], has_subject_anchor: bool) -> float:
    confidence = float(row.get("confidence") or 0.55)
    if has_subject_anchor:
        confidence += 0.08
    if row.get("canonical_person_id"):
        confidence += 0.04
    return round(min(0.86, max(0.3, confidence)), 3)


def _should_quarantine_claim(row: dict[str, Any]) -> bool:
    text = str(row.get("text") or "").strip()
    label_pack = str(row.get("label_pack") or "")
    if is_template_claim(text):
        return True
    if str(row.get("label_status") or "").startswith("failed"):
        return True
    if label_pack in LEGACY_LABEL_PACKS and _looks_like_legacy_fragment(text, label_pack):
        return True
    return False


def _should_include_event_observation(row: dict[str, Any]) -> bool:
    event_type = str(row.get("event_type") or "")
    if event_type not in EVENT_OBSERVATION_TYPES:
        return False
    if str(row.get("label_status") or "").startswith(("failed", "quarantined")):
        return False
    concept = _phrase_from_identifier(row.get("concept_name"))
    frame = _phrase_from_identifier(row.get("frame"))
    surface_terms = loads_json(row.get("surface_terms_json"), default=[]) or []
    return bool(concept or frame or surface_terms)


def _looks_like_legacy_fragment(text: str, label_pack: str) -> bool:
    normalized = " ".join(text.strip().split())
    if len(normalized) < 35:
        return True
    if label_pack == "ai_discourse_v3" and re.match(r"(?i)^segment\b", normalized):
        return True
    if label_pack == "ai_discourse_v2":
        return True
    return False


def _status_for_subject(items: list[dict[str, Any]]) -> str:
    return "candidate" if len(items) >= 1 else "needs_review"


def _subject_evidence(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "method": METHOD,
        "claim_count": sum(1 for item in items if item.get("claim_id")),
        "event_observation_count": sum(1 for item in items if item.get("discourse_event_id") and not item.get("claim_id")),
        "claim_ids": sorted(str(item["claim_id"]) for item in items if item.get("claim_id")),
        "discourse_event_ids": sorted(str(item["discourse_event_id"]) for item in items if item.get("discourse_event_id"))[:200],
        "variant_count": len({item["variant_key"] for item in items if item.get("variant_key")}),
        "source_count": len({item.get("source_id") for item in items if item.get("source_id")}),
        "episode_count": len({item.get("episode_id") for item in items if item.get("episode_id")}),
        "stance_counts": dict(sorted(Counter(str(item.get("stance") or "unspecified") for item in items).items())),
        "frame_counts": dict(sorted(Counter(str(item.get("frame") or "unspecified") for item in items).items())),
        "event_role_counts": dict(sorted(Counter(str(item.get("event_role") or "claim") for item in items).items())),
        "label_pack_counts": dict(sorted(Counter(str(item.get("label_pack") or "unknown") for item in items).items())),
        "legacy_observations": sum(1 for item in items if item.get("legacy_adapter")),
        "current_observations": sum(1 for item in items if not item.get("legacy_adapter")),
        "first_published_at": min((str(item.get("published_at") or "")[:10] for item in items if item.get("published_at")), default=None),
        "last_published_at": max((str(item.get("published_at") or "")[:10] for item in items if item.get("published_at")), default=None),
    }


def _variant_evidence(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "method": METHOD,
        "claim_count": len(items),
        "claim_ids": sorted(str(item["claim_id"]) for item in items),
        "stance_counts": dict(sorted(Counter(str(item.get("stance") or "unspecified") for item in items).items())),
        "label_pack_counts": dict(sorted(Counter(str(item.get("label_pack") or "unknown") for item in items).items())),
        "legacy_observations": sum(1 for item in items if item.get("legacy_adapter")),
        "first_published_at": min((str(item.get("published_at") or "")[:10] for item in items if item.get("published_at")), default=None),
        "last_published_at": max((str(item.get("published_at") or "")[:10] for item in items if item.get("published_at")), default=None),
    }


def _observation_evidence(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "method": METHOD,
        "claim_text": item.get("claim_text"),
        "event_type": item.get("event_type"),
        "concept_name": item.get("concept_name"),
        "source_name": item.get("source_name"),
        "label_pack": item.get("label_pack"),
        "label_status": item.get("label_status"),
        "legacy_adapter": bool(item.get("legacy_adapter")),
    }


def _event_observation_evidence(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "method": METHOD,
        "event_type": item.get("event_type"),
        "event_role": item.get("event_role"),
        "concept_name": item.get("concept_name"),
        "source_name": item.get("source_name"),
        "label_pack": item.get("label_pack"),
        "label_status": item.get("label_status"),
    }


def _rebuild_expert_positions(conn: sqlite3.Connection, *, run_id: str, ts: str) -> int:
    conn.execute("DELETE FROM claim_subject_expert_positions WHERE method = ?", (METHOD,))
    observations: dict[tuple[str, str, str], dict[str, Any]] = {}
    rows = conn.execute(
        """
        SELECT
          claim_position_observations.id AS observation_id,
          'claim' AS observation_kind,
          claim_position_observations.subject_id,
          claim_position_observations.canonical_person_id,
          canonical_people.display_name AS canonical_person,
          claim_position_observations.speaker_name,
          claim_position_observations.speaker_affiliation,
          claim_position_observations.stance,
          claim_position_observations.source_id,
          claim_position_observations.episode_id,
          claim_position_observations.published_at,
          claim_position_observations.confidence
        FROM claim_position_observations
        LEFT JOIN canonical_people ON canonical_people.id = claim_position_observations.canonical_person_id
        WHERE claim_position_observations.method = ?
        UNION ALL
        SELECT
          claim_subject_event_observations.id AS observation_id,
          'event' AS observation_kind,
          claim_subject_event_observations.subject_id,
          claim_subject_event_observations.canonical_person_id,
          canonical_people.display_name AS canonical_person,
          claim_subject_event_observations.speaker_name,
          claim_subject_event_observations.speaker_affiliation,
          claim_subject_event_observations.stance,
          claim_subject_event_observations.source_id,
          claim_subject_event_observations.episode_id,
          claim_subject_event_observations.published_at,
          claim_subject_event_observations.confidence
        FROM claim_subject_event_observations
        LEFT JOIN canonical_people ON canonical_people.id = claim_subject_event_observations.canonical_person_id
        WHERE claim_subject_event_observations.method = ?
        """,
        (METHOD, METHOD),
    ).fetchall()
    for row in rows:
        speaker_name = str(row["canonical_person"] or row["speaker_name"] or "unknown").strip() or "unknown"
        speaker_key = str(row["canonical_person_id"] or f"surface:{_key(speaker_name)}")
        stance = str(row["stance"] or "unspecified")
        key = (row["subject_id"], speaker_key, stance)
        bucket = observations.setdefault(
            key,
            {
                "subject_id": row["subject_id"],
                "canonical_person_id": row["canonical_person_id"],
                "speaker_key": speaker_key,
                "speaker_name": speaker_name[:160],
                "speaker_affiliation": row["speaker_affiliation"],
                "stance": stance,
                "observation_count": 0,
                "claim_observation_count": 0,
                "event_observation_count": 0,
                "sources": set(),
                "episodes": set(),
                "published": [],
                "confidences": [],
                "claim_observation_ids": [],
                "event_observation_ids": [],
            },
        )
        bucket["observation_count"] += 1
        if row["observation_kind"] == "claim":
            bucket["claim_observation_count"] += 1
            bucket["claim_observation_ids"].append(row["observation_id"])
        else:
            bucket["event_observation_count"] += 1
            bucket["event_observation_ids"].append(row["observation_id"])
        if row["source_id"]:
            bucket["sources"].add(row["source_id"])
        if row["episode_id"]:
            bucket["episodes"].add(row["episode_id"])
        if row["published_at"]:
            bucket["published"].append(str(row["published_at"])[:10])
        if row["confidence"] is not None:
            bucket["confidences"].append(float(row["confidence"]))

    for item in observations.values():
        confidence = round(sum(item["confidences"]) / len(item["confidences"]), 3) if item["confidences"] else 0.5
        evidence = {
            "method": METHOD,
            "claim_observation_ids": sorted(item["claim_observation_ids"])[:100],
            "event_observation_ids": sorted(item["event_observation_ids"])[:100],
            "source_count": len(item["sources"]),
            "episode_count": len(item["episodes"]),
            "canonical_person_id": item["canonical_person_id"],
        }
        conn.execute(
            """
            INSERT INTO claim_subject_expert_positions
              (id, subject_id, canonical_person_id, speaker_key, speaker_name, speaker_affiliation, stance,
               observation_count, claim_observation_count, event_observation_count, source_count, episode_count,
               first_published_at, last_published_at, confidence, status, evidence_json, method, run_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'candidate', ?, ?, ?, ?, ?)
            ON CONFLICT(subject_id, speaker_key, stance, method) DO UPDATE SET
              canonical_person_id = excluded.canonical_person_id,
              speaker_name = excluded.speaker_name,
              speaker_affiliation = excluded.speaker_affiliation,
              observation_count = excluded.observation_count,
              claim_observation_count = excluded.claim_observation_count,
              event_observation_count = excluded.event_observation_count,
              source_count = excluded.source_count,
              episode_count = excluded.episode_count,
              first_published_at = excluded.first_published_at,
              last_published_at = excluded.last_published_at,
              confidence = excluded.confidence,
              status = excluded.status,
              evidence_json = excluded.evidence_json,
              run_id = excluded.run_id,
              updated_at = excluded.updated_at
            """,
            (
                stable_id("claim_subject_expert_position", item["subject_id"], item["speaker_key"], item["stance"], prefix="csep_"),
                item["subject_id"],
                item["canonical_person_id"],
                item["speaker_key"],
                item["speaker_name"],
                item["speaker_affiliation"],
                item["stance"],
                item["observation_count"],
                item["claim_observation_count"],
                item["event_observation_count"],
                len(item["sources"]),
                len(item["episodes"]),
                min(item["published"]) if item["published"] else None,
                max(item["published"]) if item["published"] else None,
                confidence,
                dumps_json(evidence),
                METHOD,
                run_id,
                ts,
                ts,
            ),
        )
    return len(observations)
