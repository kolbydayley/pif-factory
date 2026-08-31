from __future__ import annotations

import re
import sqlite3
import time
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

from .util import dumps_json, now_iso, stable_id


METHOD = "precision_first_topic_issue_v1"
JUNK_TOPICS = {"other", "misc", "miscellaneous", "unknown", "none", "n a", "general"}
STOPWORDS = {"a", "an", "and", "as", "for", "in", "of", "on", "the", "to", "vs"}


def normalize_topic_surface(value: str | None) -> str | None:
    """Normalize typography without erasing meaningful issue scope."""

    if not value:
        return None
    text = unicodedata.normalize("NFKC", str(value)).lower()
    text = text.replace("’", "'").replace("‘", "'")
    text = re.sub(r"\b([a-z0-9]+)'s\b", r"\1", text)
    text = re.sub(r"[\s_/-]+", " ", text)
    text = re.sub(r"[^a-z0-9 .+#]", "", text)
    text = " ".join(text.split())
    return text or None


def _stem_token(token: str) -> str:
    # Deliberately narrow: plural morphology is safe; semantic stemming is not.
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 4 and token.endswith("es"):
        return token[:-2]
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token


def topic_tokens(value: str) -> tuple[str, ...]:
    normalized = normalize_topic_surface(value) or ""
    return tuple(
        _stem_token(token)
        for token in normalized.split()
        if token not in STOPWORDS
    )


@dataclass(frozen=True)
class PairDecision:
    relation: str
    confidence: float
    auto_merge: bool
    rationale: str


def classify_topic_pair(subject: str, object_: str) -> PairDecision:
    """Classify lexical evidence, merging only identity-level variants."""

    left = normalize_topic_surface(subject) or ""
    right = normalize_topic_surface(object_) or ""
    if not left or not right:
        return PairDecision("uncertain", 0.0, False, "empty normalized topic")
    if left == right:
        return PairDecision("same_issue", 1.0, True, "exact normalized surface")

    left_tokens = topic_tokens(left)
    right_tokens = topic_tokens(right)
    left_set, right_set = set(left_tokens), set(right_tokens)
    if left_tokens and len(left_tokens) == len(right_tokens) and left_set == right_set:
        return PairDecision("same_issue", 0.99, True, "token order or plural morphology only")

    if left_set and right_set and left_set > right_set:
        return PairDecision("narrower_than", 0.9, False, "contains the broader issue plus scope")
    if left_set and right_set and left_set < right_set:
        return PairDecision("broader_than", 0.9, False, "is contained by a more specific issue")

    overlap = len(left_set & right_set) / len(left_set | right_set) if left_set | right_set else 0.0
    if overlap >= 0.6:
        return PairDecision("related_to", round(overlap, 3), False, "substantial token overlap")
    return PairDecision("distinct_from", round(1.0 - overlap, 3), False, "insufficient identity evidence")


def build_precision_first_canon(counts: Counter[str]) -> dict[str, str]:
    """Return a deterministic display-name map with no scope-erasing merges."""

    names = [name for name, _count in counts.most_common() if name not in JUNK_TOPICS]
    signature_heads: dict[tuple[str, ...], str] = {}
    canonical: dict[str, str] = {}
    for name in names:
        # The only automatic fuzzy merge is an identical bag of tokens after
        # narrow plural normalization. This stays O(vocabulary), even for the
        # dashboard's 400k-term long tail.
        signature = tuple(sorted(topic_tokens(name)))
        merged = signature_heads.get(signature) if signature else None
        canonical[name] = merged or name
        if merged is None and signature:
            signature_heads[signature] = name
    return canonical


def load_accepted_topic_registry(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Load current accepted assignments; an unmigrated fixture safely returns empty."""

    try:
        rows = conn.execute(
            """
            WITH current_assignment AS (
              SELECT normalized_topic, MAX(decision_version) AS decision_version
              FROM topic_issue_assignments
              WHERE status = 'accepted' AND relation_type = 'same_issue'
              GROUP BY normalized_topic
            )
            SELECT tia.normalized_topic, ci.id AS issue_id, ci.display_name,
                   cia.alias, cia.normalized_alias
            FROM current_assignment ca
            JOIN topic_issue_assignments tia
              ON tia.normalized_topic = ca.normalized_topic
             AND tia.decision_version = ca.decision_version
             AND tia.status = 'accepted'
            JOIN canonical_issues ci ON ci.id = tia.issue_id
            LEFT JOIN canonical_issue_aliases cia
              ON cia.issue_id = ci.id AND cia.status = 'accepted'
            WHERE ci.status = 'accepted'
            ORDER BY tia.normalized_topic, cia.alias
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return {}
        raise

    registry: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row["normalized_topic"])
        item = registry.setdefault(
            key,
            {"issue_id": row["issue_id"], "display_name": row["display_name"], "aliases": []},
        )
        alias = row["alias"]
        if alias and alias != item["display_name"] and alias not in item["aliases"]:
            item["aliases"].append(alias)
    return registry


def _topic_observations(conn: sqlite3.Connection, *, limit: int) -> list[dict[str, Any]]:
    observations: dict[str, dict[str, Any]] = {}

    def add(raw: Any, count: int, episodes: int, shows: int) -> None:
        normalized = normalize_topic_surface(raw)
        if not normalized or normalized in JUNK_TOPICS:
            return
        item = observations.setdefault(
            normalized,
            {"normalized": normalized, "count": 0, "episodes": 0, "shows": 0, "surfaces": Counter()},
        )
        item["count"] += int(count or 0)
        item["episodes"] = max(item["episodes"], int(episodes or 0))
        item["shows"] = max(item["shows"], int(shows or 0))
        item["surfaces"][str(raw).strip()] += int(count or 0)

    for row in conn.execute(
        """
        SELECT json_extract(t.value, '$.topic') AS raw_topic,
               COUNT(*) AS mentions,
               COUNT(DISTINCT s.episode_id) AS episodes,
               COUNT(DISTINCT s.source_id) AS shows
        FROM labels l
        JOIN segments s ON s.id = l.segment_id,
             json_each(l.output_json, '$.topics') t
        WHERE json_extract(t.value, '$.topic') IS NOT NULL
        GROUP BY raw_topic
        """
    ):
        add(row["raw_topic"], row["mentions"], row["episodes"], row["shows"])
    for row in conn.execute(
        """
        SELECT ap.concept_name AS raw_topic, COUNT(*) AS mentions,
               COUNT(DISTINCT s.episode_id) AS episodes,
               COUNT(DISTINCT s.source_id) AS shows
        FROM actor_positions ap
        JOIN segments s ON s.id = ap.segment_id
        WHERE ap.concept_name IS NOT NULL
        GROUP BY ap.concept_name
        """
    ):
        add(row["raw_topic"], row["mentions"], row["episodes"], row["shows"])

    ranked = sorted(observations.values(), key=lambda item: (-item["count"], item["normalized"]))
    return ranked[:limit]


def _best_related_head(
    name: str,
    counts: Counter[str],
    token_index: dict[str, list[str]],
) -> tuple[str, PairDecision] | None:
    candidates: set[str] = set()
    for token in topic_tokens(name):
        candidates.update(token_index.get(token, ()))
    decisions = [(head, classify_topic_pair(name, head)) for head in candidates]
    decisions = [item for item in decisions if item[1].relation != "distinct_from"]
    if not decisions:
        return None
    decisions.sort(key=lambda item: (item[1].auto_merge, item[1].confidence, counts[item[0]], item[0]), reverse=True)
    return decisions[0]


def canonicalize_topics(
    conn: sqlite3.Connection,
    *,
    limit: int = 5000,
    dry_run: bool = False,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    started_at = now_iso()
    observations = _topic_observations(conn, limit=limit)
    existing_registry = load_accepted_topic_registry(conn)
    counts: Counter[str] = Counter({item["normalized"]: item["count"] for item in observations})
    heads: list[str] = []
    head_issue_ids: dict[str, str] = {}
    token_index: dict[str, list[str]] = defaultdict(list)
    decisions: list[dict[str, Any]] = []

    for item in observations:
        name = item["normalized"]
        registered = existing_registry.get(name)
        if registered:
            head = str(registered["display_name"])
            issue_id = str(registered["issue_id"])
            decision = PairDecision("same_issue", 1.0, True, "persisted accepted assignment")
            related = None
            if head not in head_issue_ids:
                heads.append(head)
                head_issue_ids[head] = issue_id
                for token in set(topic_tokens(head)):
                    token_index[token].append(head)
        else:
            related = _best_related_head(name, counts, token_index)
        if not registered and related and related[1].auto_merge:
            head, decision = related
            issue_id = head_issue_ids[head]
        elif not registered:
            head = name
            issue_id = stable_id("canonical_issue", head, prefix="iss_")
            decision = PairDecision("same_issue", 1.0, True, "new stable issue")
            heads.append(name)
            head_issue_ids[head] = issue_id
            for token in set(topic_tokens(name)):
                token_index[token].append(name)
        decisions.append(
            {
                "item": item,
                "head": head,
                "issue_id": issue_id,
                "decision": decision,
                "related": related,
            }
        )

    metrics = {
        "ok": True,
        "method": METHOD,
        "topics_seen": len(observations),
        "canonical_issues": len(heads),
        "identity_merges": sum(1 for row in decisions if row["head"] != row["item"]["normalized"]),
        "scoped_relations": sum(
            1
            for row in decisions
            if row["related"] is not None and not row["related"][1].auto_merge
        ),
        "dry_run": dry_run,
    }
    if dry_run:
        return metrics

    run_id = stable_id(
        "topic_canonicalization_run",
        METHOD,
        started_at,
        str(limit),
        str(time.time_ns()),
        prefix="tcr_",
    )
    conn.execute(
        """
        INSERT INTO topic_canonicalization_runs
          (id, method, status, parameters_json, metrics_json, started_at, completed_at, created_at, updated_at)
        VALUES (?, ?, 'running', ?, '{}', ?, NULL, ?, ?)
        """,
        (run_id, METHOD, dumps_json({"limit": limit}), started_at, started_at, started_at),
    )
    ts = now_iso()
    try:
        for row in decisions:
            item = row["item"]
            head = row["head"]
            issue_id = row["issue_id"]
            conn.execute(
                """
                INSERT INTO canonical_issues
                  (id, display_name, normalized_name, status, version, created_at, updated_at)
                VALUES (?, ?, ?, 'accepted', 1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at
                """,
                (issue_id, head, head, ts, ts),
            )
            surfaces: Counter[str] = item["surfaces"]
            display_surface = surfaces.most_common(1)[0][0] if surfaces else item["normalized"]
            alias_id = stable_id("canonical_issue_alias", issue_id, item["normalized"], prefix="isa_")
            conn.execute(
                """
                INSERT INTO canonical_issue_aliases
                  (id, issue_id, alias, normalized_alias, status, confidence, method,
                   evidence_count, source_diversity, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'accepted', ?, ?, ?, ?, ?, ?)
                ON CONFLICT(issue_id, normalized_alias) DO UPDATE SET
                  alias = excluded.alias,
                  evidence_count = excluded.evidence_count,
                  source_diversity = excluded.source_diversity,
                  updated_at = excluded.updated_at
                """,
                (
                    alias_id,
                    issue_id,
                    display_surface,
                    item["normalized"],
                    row["decision"].confidence,
                    METHOD,
                    item["count"],
                    item["shows"],
                    ts,
                    ts,
                ),
            )
            assignment_id = stable_id("topic_issue_assignment", item["normalized"], "1", prefix="tia_")
            conn.execute(
                """
                INSERT INTO topic_issue_assignments
                  (id, raw_topic, normalized_topic, issue_id, relation_type, status,
                   confidence, method, evidence_count, source_diversity,
                   decision_version, evidence_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'same_issue', 'accepted', ?, ?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(normalized_topic, decision_version) DO UPDATE SET
                  raw_topic = excluded.raw_topic,
                  issue_id = excluded.issue_id,
                  confidence = excluded.confidence,
                  evidence_count = excluded.evidence_count,
                  source_diversity = excluded.source_diversity,
                  evidence_json = excluded.evidence_json,
                  updated_at = excluded.updated_at
                """,
                (
                    assignment_id,
                    display_surface,
                    item["normalized"],
                    issue_id,
                    row["decision"].confidence,
                    METHOD,
                    item["count"],
                    item["shows"],
                    dumps_json({"episodes": item["episodes"], "surface_count": len(surfaces)}),
                    ts,
                    ts,
                ),
            )
            related = row["related"]
            if related is not None and not related[1].auto_merge:
                object_head, relation = related
                object_id = head_issue_ids[object_head]
                relation_id = stable_id(
                    "canonical_issue_relation", issue_id, object_id, relation.relation, "1", prefix="isr_"
                )
                conn.execute(
                    """
                    INSERT INTO canonical_issue_relations
                      (id, subject_issue_id, object_issue_id, relation_type, status,
                       confidence, method, evidence_json, version, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'candidate', ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(subject_issue_id, object_issue_id, relation_type, version)
                    DO UPDATE SET confidence = excluded.confidence,
                                  evidence_json = excluded.evidence_json,
                                  updated_at = excluded.updated_at
                    """,
                    (
                        relation_id,
                        issue_id,
                        object_id,
                        relation.relation,
                        relation.confidence,
                        METHOD,
                        dumps_json({"rationale": relation.rationale}),
                        ts,
                        ts,
                    ),
                )
        completed = now_iso()
        conn.execute(
            """
            UPDATE topic_canonicalization_runs
            SET status = 'completed', metrics_json = ?, completed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (dumps_json(metrics), completed, completed, run_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    metrics["run_id"] = run_id
    return metrics
