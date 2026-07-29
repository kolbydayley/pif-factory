from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from itertools import combinations
from typing import Any

from .util import dumps_json, now_iso, stable_id


METHOD = "deterministic_canonical_claim_v1"
SEMANTIC_METHOD = "local_semantic_claim_judge_v1"
RELATION = "supports_canonical_claim"

_GENERIC_PATTERNS = [
    re.compile(r"^\s*(this\s+)?segment\s+(contains|discusses|mentions|covers|touches|frames|raises|signals|includes)\b", re.I),
    re.compile(r"^\s*(this\s+)?clip\s+(contains|discusses|mentions|covers|touches|frames|raises|signals|includes)\b", re.I),
    re.compile(r"^\s*(the\s+)?conversation\s+(contains|discusses|mentions|covers|touches|frames|raises|signals|includes)\b", re.I),
    re.compile(r"^\s*(the\s+)?speaker\s+(offers|treats|frames|makes)\s+.*\b(evidence|passing reference|forward-looking claim|downstream effects)\b", re.I),
    re.compile(r"^\s*(the\s+)?speaker\s+frames\s+.+\s+as\s+a\s+deployment\s+or\s+governance\s+concern\b", re.I),
    re.compile(r"^\s*(the\s+)?speaker\s+makes\s+a\s+concrete\s+capability\s+claim\s+centered\s+on\b", re.I),
    re.compile(r"\baround\s+[a-z0-9 _-]{2,80}\s*$", re.I),
]

_ATTRIBUTION_PATTERN = re.compile(
    r"^\s*(?:the\s+)?(?:host|guest|speaker|interviewer|interviewee|they|he|she|we|i|[A-Z][A-Za-z .'-]{1,80})\s+"
    r"(?:says|said|argues|argued|claims|claimed|believes|believed|thinks|thought|predicts|predicted|notes|noted|reports|reported|suggests|suggested)\s+"
    r"(?:that\s+)?",
)

_STOPWORDS = {
    "a",
    "access",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "because",
    "by",
    "can",
    "could",
    "for",
    "from",
    "get",
    "gets",
    "have",
    "has",
    "in",
    "into",
    "is",
    "it",
    "its",
    "lead",
    "may",
    "made",
    "make",
    "makes",
    "might",
    "more",
    "of",
    "on",
    "or",
    "should",
    "than",
    "that",
    "the",
    "their",
    "there",
    "this",
    "to",
    "team",
    "teams",
    "user",
    "users",
    "will",
    "with",
    "would",
}

_TOKEN_SYNONYMS = {
    "agentic": "agent",
    "agents": "agent",
    "automating": "automate",
    "automation": "automate",
    "closed": "proprietary",
    "closedsource": "proprietary",
    "coding": "code",
    "coders": "developer",
    "developers": "developer",
    "devs": "developer",
    "engineers": "engineer",
    "llms": "llm",
    "models": "model",
    "opensource": "open",
    "open": "open",
    "peak": "frontier",
    "productivity": "productive",
    "proprietary": "proprietary",
    "workers": "worker",
}

_TEMPORAL_TOKENS = {
    "today",
    "tomorrow",
    "yesterday",
    "current",
    "currently",
    "future",
    "near",
    "long",
    "short",
    "year",
    "years",
    "month",
    "months",
    "week",
    "weeks",
}

_BLOCK_STOPWORDS = {
    "about",
    "argue",
    "claim",
    "frame",
    "says",
    "say",
    "speaker",
    "thing",
    "people",
    "person",
    "what",
}


def canonicalize_claims(
    conn: sqlite3.Connection,
    *,
    pilot_id: str | None = None,
    scope: str = "last_18_months",
    model: str = "deterministic-local",
    limit: int | None = None,
    dry_run: bool = False,
    mode: str = "deterministic",
) -> dict[str, Any]:
    if scope not in {"last_18_months", "all"}:
        raise ValueError("scope must be last_18_months or all")
    if mode not in {"deterministic", "semantic"}:
        raise ValueError("mode must be deterministic or semantic")
    started_at = now_iso()
    run_id = stable_id("claim_canonicalization_run", started_at, scope, model, pilot_id or "all", prefix="ccr_")
    method = SEMANTIC_METHOD if mode == "semantic" else METHOD
    params = {"pilot_id": pilot_id, "scope": scope, "limit": limit, "method": method, "mode": mode}
    rows = _claim_rows(conn, pilot_id=pilot_id, scope=scope, limit=limit)
    group_result = _build_claim_groups(rows, mode=mode)
    groups = group_result["groups"]

    metrics = {
        "ok": True,
        "run_id": run_id,
        "scope": scope,
        "model": model,
        "method": method,
        "mode": mode,
        "claims_seen": len(rows),
        "claims_grouped": sum(len(group["items"]) for group in groups),
        "template_claims_quarantined": group_result["template_claims_quarantined"],
        "empty_claims_skipped": group_result["empty_claims_skipped"],
        "candidate_blocks": group_result["candidate_blocks"],
        "semantic_pairs_considered": group_result["semantic_pairs_considered"],
        "semantic_merge_count": sum(max(0, len(group["items"]) - 1) for group in groups if group["status"] == "semantic_candidate"),
        "clusters_considered": len(groups),
        "clusters_upserted": 0,
        "memberships_upserted": 0,
        "multi_claim_clusters": sum(1 for group in groups if len(group["items"]) > 1),
        "needs_review_clusters": sum(1 for group in groups if group["status"] == "needs_review"),
        "low_confidence_clusters": sum(1 for group in groups if group["confidence"] < 0.7),
        "dry_run": dry_run,
    }
    if dry_run:
        return metrics

    conn.execute(
        """
        INSERT INTO claim_canonicalization_runs
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
        processed_claim_ids = [str(row["claim_id"]) for row in rows if row.get("claim_id")]
        _delete_existing_memberships(conn, processed_claim_ids)
        for group in groups:
            key = group["key"]
            items = group["items"]
            cluster_id = stable_id("claim_cluster", method, key, prefix="ccl_")
            canonical = _canonical_claim_text(items)
            concept_id = _majority(items, "concept_id")
            evidence = _cluster_evidence(items, key=key, method=method)
            status = group["status"]
            confidence = group["confidence"]
            conn.execute(
                """
                INSERT INTO claim_clusters
                  (id, canonical_claim_text, concept_id, status, judge_model, confidence, evidence_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  canonical_claim_text = excluded.canonical_claim_text,
                  concept_id = COALESCE(excluded.concept_id, claim_clusters.concept_id),
                  status = excluded.status,
                  judge_model = excluded.judge_model,
                  confidence = excluded.confidence,
                  evidence_json = excluded.evidence_json,
                  updated_at = excluded.updated_at
                """,
                (cluster_id, canonical, concept_id, status, model, confidence, dumps_json(evidence), ts, ts),
            )
            metrics["clusters_upserted"] += 1
            for item in items:
                conn.execute(
                    """
                    INSERT INTO claim_cluster_members
                      (id, cluster_id, claim_id, relation, confidence, method, run_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cluster_id, claim_id, relation) DO UPDATE SET
                      confidence = excluded.confidence,
                      method = excluded.method,
                      run_id = excluded.run_id,
                      updated_at = excluded.updated_at
                    """,
                    (
                        stable_id("claim_cluster_member", cluster_id, item["claim_id"], RELATION, prefix="ccm_"),
                        cluster_id,
                        item["claim_id"],
                        RELATION,
                        min(0.92, max(0.55, confidence)),
                        method,
                        run_id,
                        ts,
                        ts,
                    ),
                )
                metrics["memberships_upserted"] += 1
        completed_at = now_iso()
        conn.execute(
            """
            UPDATE claim_canonicalization_runs
            SET status = 'completed',
                metrics_json = ?,
                completed_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (dumps_json(metrics), completed_at, completed_at, run_id),
        )
        conn.commit()
    except Exception as exc:
        failed_at = now_iso()
        metrics["ok"] = False
        metrics["error"] = str(exc)[:500]
        conn.execute(
            """
            UPDATE claim_canonicalization_runs
            SET status = 'failed',
                metrics_json = ?,
                completed_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (dumps_json(metrics), failed_at, failed_at, run_id),
        )
        conn.commit()
        raise
    return metrics


def is_template_claim(text: str) -> bool:
    normalized = " ".join(text.strip().lower().split())
    if len(normalized) < 12:
        return True
    return any(pattern.search(normalized) for pattern in _GENERIC_PATTERNS)


def canonical_claim_key(claim: dict[str, Any]) -> str:
    tokens = _important_tokens(str(claim.get("text") or ""))
    if not tokens:
        return "empty_claim"
    event_type = _normalize_value(claim.get("event_type"))
    numbers = sorted({token for token in tokens if any(ch.isdigit() for ch in token)})
    words = sorted({token for token in tokens if not any(ch.isdigit() for ch in token)})
    return "|".join(
        part
        for part in [
            event_type,
            ",".join(numbers[:8]),
            ",".join(words[:24]),
        ]
        if part
    )


def _build_claim_groups(rows: list[dict[str, Any]], *, mode: str) -> dict[str, Any]:
    candidate_rows: list[dict[str, Any]] = []
    quarantined = 0
    empty = 0
    for row in rows:
        text = str(row.get("text") or "").strip()
        if not text:
            empty += 1
            continue
        if is_template_claim(text):
            quarantined += 1
            continue
        if not _important_tokens(text):
            empty += 1
            continue
        candidate_rows.append(row)
    if mode == "deterministic":
        keyed: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in candidate_rows:
            key = canonical_claim_key(row)
            if key == "empty_claim":
                empty += 1
                continue
            keyed[key].append(row)
        groups = [
            {
                "key": key,
                "items": items,
                "status": "canonical_candidate" if len(items) > 1 else "singleton_candidate",
                "confidence": _cluster_confidence(items),
            }
            for key, items in keyed.items()
        ]
        return {
            "groups": groups,
            "template_claims_quarantined": quarantined,
            "empty_claims_skipped": empty,
            "candidate_blocks": len(keyed),
            "semantic_pairs_considered": 0,
        }
    blocks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidate_rows:
        for block_key in _semantic_block_keys(row):
            blocks[block_key].append(row)
    components, pairs_considered = _semantic_components_from_blocks(candidate_rows, blocks)
    groups: list[dict[str, Any]] = []
    for items in components:
        confidence = _semantic_cluster_confidence(items)
        status = "singleton_candidate"
        if len(items) > 1:
            status = "semantic_candidate" if confidence >= 0.76 else "needs_review"
        groups.append(
            {
                "key": _semantic_group_key(items),
                "items": items,
                "status": status,
                "confidence": confidence,
            }
        )
    return {
        "groups": groups,
        "template_claims_quarantined": quarantined,
        "empty_claims_skipped": empty,
        "candidate_blocks": len(blocks),
        "semantic_pairs_considered": pairs_considered,
    }


def _semantic_block_key(row: dict[str, Any]) -> str:
    event_type = _normalize_value(row.get("event_type")) or "unknown_event"
    concept_tokens = _important_tokens(str(row.get("concept_name") or "").replace("_", " "))
    claim_tokens = _important_tokens(str(row.get("text") or ""))
    numbers = ",".join(_numeric_signature(str(row.get("text") or ""))[:6])
    if concept_tokens:
        anchor = ",".join(sorted(set(concept_tokens))[:8])
    else:
        anchor = ",".join(sorted(set(claim_tokens))[:5])
    return "|".join(part for part in [event_type, anchor, numbers] if part)


def _semantic_block_keys(row: dict[str, Any]) -> list[str]:
    keys = [_semantic_block_key(row)]
    event_type = _normalize_value(row.get("event_type")) or "unknown_event"
    numbers = ",".join(_numeric_signature(str(row.get("text") or ""))[:6])
    tokens = []
    seen: set[str] = set()
    for token in _important_tokens(str(row.get("text") or "")):
        if token in seen or token in _BLOCK_STOPWORDS:
            continue
        seen.add(token)
        tokens.append(token)
    for left, right in combinations(tokens[:8], 2):
        keys.append("|".join(part for part in [event_type, "tok2", ",".join(sorted((left, right))), numbers] if part))
    return keys


def _semantic_components_from_blocks(rows: list[dict[str, Any]], blocks: dict[str, list[dict[str, Any]]]) -> tuple[list[list[dict[str, Any]]], int]:
    if len(rows) <= 1:
        return [[row] for row in rows], 0
    index_by_claim_id = {str(row["claim_id"]): index for index, row in enumerate(rows)}
    parent = list(range(len(rows)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    features = [_semantic_features(row) for row in rows]
    pairs = 0
    seen_pairs: set[tuple[int, int]] = set()
    for block_rows in blocks.values():
        if len(block_rows) < 2 or len(block_rows) > 160:
            continue
        block_indexes = sorted(index_by_claim_id[str(row["claim_id"])] for row in block_rows)
        for left, right in combinations(block_indexes, 2):
            pair = (left, right)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            pairs += 1
            if _semantic_match(features[left], features[right]):
                union(left, right)
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, row in enumerate(rows):
        grouped[find(index)].append(row)
    return list(grouped.values()), pairs


def _semantic_features(row: dict[str, Any]) -> dict[str, Any]:
    text = str(row.get("text") or "")
    tokens = set(_important_tokens(text))
    return {
        "tokens": tokens,
        "numbers": set(_numeric_signature(text)),
        "negation": bool(re.search(r"\b(no|not|never|without|cannot|can't|won't|isn't|aren't)\b", text.lower())),
        "entities": _entity_signature(text),
        "temporal": {token for token in tokens if token in _TEMPORAL_TOKENS or re.fullmatch(r"20\d{2}", token)},
        "event_type": _normalize_value(row.get("event_type")),
    }


def _semantic_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if left["event_type"] and right["event_type"] and left["event_type"] != right["event_type"]:
        return False
    if left["numbers"] != right["numbers"]:
        return False
    if left["negation"] != right["negation"]:
        return False
    if left["temporal"] and right["temporal"] and left["temporal"] != right["temporal"]:
        return False
    left_entities = left["entities"]
    right_entities = right["entities"]
    if left_entities and right_entities and left_entities.isdisjoint(right_entities):
        return False
    left_tokens = left["tokens"]
    right_tokens = right["tokens"]
    if not left_tokens or not right_tokens:
        return False
    overlap = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    jaccard = overlap / union if union else 0
    containment = overlap / min(len(left_tokens), len(right_tokens))
    return jaccard >= 0.62 or containment >= 0.8


def _semantic_group_key(items: list[dict[str, Any]]) -> str:
    canonical = min(items, key=lambda item: (len(str(item.get("text") or "")), str(item.get("claim_id") or "")))
    return canonical_claim_key(canonical)


def _semantic_cluster_confidence(items: list[dict[str, Any]]) -> float:
    if len(items) <= 1:
        return 0.58
    token_sets = [set(_important_tokens(str(item.get("text") or ""))) for item in items]
    similarities: list[float] = []
    for left in range(len(token_sets)):
        for right in range(left + 1, len(token_sets)):
            overlap = len(token_sets[left] & token_sets[right])
            union = len(token_sets[left] | token_sets[right])
            if union:
                jaccard = overlap / union
                containment = overlap / min(len(token_sets[left]), len(token_sets[right]))
                similarities.append(max(jaccard, containment))
    mean_similarity = sum(similarities) / len(similarities) if similarities else 0.8
    source_count = len({item.get("source_id") for item in items if item.get("source_id")})
    confidence = 0.68 + min(0.2, max(0, mean_similarity - 0.62)) + min(0.04, source_count * 0.01)
    return round(min(0.92, confidence), 3)


def _numeric_signature(text: str) -> list[str]:
    return sorted(set(re.findall(r"\b(?:\d+(?:\.\d+)?%?|20\d{2})\b", text.lower())))


def _entity_signature(text: str) -> set[str]:
    text = _ATTRIBUTION_PATTERN.sub("", text.strip())
    entities: set[str] = set()
    for match in re.finditer(r"\b[A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*){0,3}\b", text):
        value = match.group(0).strip()
        normalized = _normalize_value(value)
        if normalized and normalized not in {"ai", "llm", "team", "teams", "user", "users"} and normalized not in _STOPWORDS and len(normalized) > 2:
            entities.add(normalized)
    return entities


def _delete_existing_memberships(conn: sqlite3.Connection, claim_ids: list[str]) -> None:
    if not claim_ids:
        return
    for index in range(0, len(claim_ids), 500):
        chunk = claim_ids[index : index + 500]
        placeholders = ",".join("?" for _ in chunk)
        conn.execute(
            f"DELETE FROM claim_cluster_members WHERE relation = ? AND claim_id IN ({placeholders})",
            (RELATION, *chunk),
        )


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
        WITH event_claims AS (
          SELECT
            label_id,
            segment_id,
            claim_text,
            MIN(event_type) AS event_type,
            MIN(canonical_concept_id) AS concept_id,
            MIN(COALESCE(NULLIF(canonical_concept_name, ''), NULLIF(candidate_concept, ''))) AS concept_name
          FROM discourse_events
          WHERE claim_text IS NOT NULL AND claim_text != ''
          GROUP BY label_id, segment_id, claim_text
        )
        SELECT
          claims.id AS claim_id,
          claims.label_id,
          claims.segment_id,
          claims.text,
          claims.stance,
          claims.confidence,
          labels.label_pack,
          segments.episode_id,
          episodes.published_at,
          sources.id AS source_id,
          sources.name AS source_name,
          event_claims.event_type,
          event_claims.concept_id,
          event_claims.concept_name
        FROM claims
        JOIN labels ON labels.id = claims.label_id
        JOIN segments ON segments.id = claims.segment_id
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        LEFT JOIN event_claims
          ON event_claims.label_id = claims.label_id
         AND event_claims.segment_id = claims.segment_id
         AND event_claims.claim_text = claims.text
        WHERE labels.label_pack = 'ai_discourse_v3_1'
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


def _important_tokens(text: str) -> list[str]:
    text = _ATTRIBUTION_PATTERN.sub("", text.strip())
    text = text.replace("open-source", "opensource").replace("closed-source", "closedsource")
    text = re.sub(r"[^A-Za-z0-9%]+", " ", text).lower()
    tokens: list[str] = []
    for token in text.split():
        if len(token) < 3 or token in _STOPWORDS:
            continue
        token = _TOKEN_SYNONYMS.get(token, token)
        if token.endswith("ies") and len(token) > 5:
            token = token[:-3] + "y"
        elif token.endswith("s") and len(token) > 4 and not token.endswith("ss"):
            token = token[:-1]
        if token and token not in _STOPWORDS:
            tokens.append(token)
    return tokens


def _normalize_value(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text[:64]


def _canonical_claim_text(items: list[dict[str, Any]]) -> str:
    candidates = [str(item.get("text") or "").strip() for item in items if str(item.get("text") or "").strip()]
    candidates.sort(key=lambda text: (len(text), text.lower()))
    return candidates[0][:1000]


def _cluster_confidence(items: list[dict[str, Any]]) -> float:
    base = 0.58 if len(items) == 1 else 0.7
    source_count = len({item.get("source_id") for item in items if item.get("source_id")})
    episode_count = len({item.get("episode_id") for item in items if item.get("episode_id")})
    return round(min(0.9, base + min(0.08, source_count * 0.02) + min(0.08, episode_count * 0.01)), 3)


def _cluster_evidence(items: list[dict[str, Any]], *, key: str, method: str = METHOD) -> dict[str, Any]:
    stance_counts = Counter(str(item.get("stance") or "unspecified") for item in items)
    published_days = sorted(
        substr
        for item in items
        if (substr := str(item.get("published_at") or "")[:10])
    )
    source_ids = {item.get("source_id") for item in items if item.get("source_id")}
    episode_ids = {item.get("episode_id") for item in items if item.get("episode_id")}
    return {
        "method": method,
        "key_sha": stable_id("claim_key", key, prefix="ck_"),
        "claim_ids": sorted(str(item["claim_id"]) for item in items),
        "claim_count": len(items),
        "source_count": len(source_ids),
        "episode_count": len(episode_ids),
        "stance_counts": dict(sorted(stance_counts.items())),
        "first_published_at": published_days[0] if published_days else None,
        "last_published_at": published_days[-1] if published_days else None,
    }


def _majority(items: list[dict[str, Any]], key: str) -> str | None:
    counts = Counter(str(item.get(key) or "") for item in items if item.get(key))
    if not counts:
        return None
    return counts.most_common(1)[0][0]
