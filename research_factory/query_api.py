from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import parse_qs, quote, urlsplit

from .accuracy import (
    AccuracyDataError,
    AccuracyUnavailableError,
    person_accuracy_history,
    person_accuracy_rankings,
    person_contrarian_success,
)
from .graph_analysis import accepted_graph_bundle
from .paths import db_path
from .util import now_iso


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
MAX_LIMIT = 200


def _limit(value: Any, default: int = 50) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, MAX_LIMIT))


@contextmanager
def open_read_only(path: str | Path) -> Iterator[sqlite3.Connection]:
    resolved = Path(path).expanduser().resolve()
    uri = f"file:{quote(str(resolved))}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    try:
        yield conn
    finally:
        conn.close()


class LocalQueryService:
    """Read-only, local-only access to private intelligence surfaces."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        enforce_query_only: bool = True,
        allow_legacy: bool = False,
    ):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.allow_legacy = allow_legacy
        if enforce_query_only:
            self.conn.execute("PRAGMA query_only = ON")

    def capabilities(self) -> dict[str, Any]:
        relations = self._relations()
        preferred = {
            "atomic_claims": "current_accepted_atomic_claims",
            "people": "current_accepted_people",
            "identity_resolutions": "current_accepted_identity_resolutions",
            "subjects": "current_accepted_claim_subjects",
            "proposition_variants": "current_accepted_proposition_variants",
            "position_observations": "current_accepted_position_observations",
            "source_affiliations": "current_accepted_source_affiliations",
            "person_appearances": "current_accepted_person_appearances",
            "claim_relations": "current_accepted_claim_relations",
            "consensus": "current_accepted_consensus_snapshots",
            "contrarian": "current_accepted_contrarian_snapshots",
            "outcomes": "current_accepted_outcome_resolutions",
            "corpus_releases": "current_accepted_corpus_releases",
        }
        surfaces = {
            "search": "current_accepted_atomic_claims" in relations or self.allow_legacy and "claims" in relations,
            "subjects": "current_accepted_claim_subjects" in relations
            or self.allow_legacy and "claim_subjects" in relations,
            "people": "current_accepted_people" in relations
            and any(
                name in relations
                for name in (
                    "current_accepted_person_appearances",
                    "current_accepted_atomic_claims",
                    "current_accepted_position_observations",
                )
            ),
            "networks": any(
                name in relations
                for name in (
                    "current_accepted_person_appearances",
                    "current_accepted_position_observations",
                    "current_accepted_source_affiliations",
                    "current_accepted_claim_relations",
                )
            ) or self.allow_legacy and any(name in relations for name in ("person_concept_edges", "person_person_mentions", "person_org_edges")),
            "consensus": any(name in relations for name in ("current_accepted_consensus_snapshots", "current_accepted_contrarian_snapshots", "current_accepted_claim_relations")),
            "outcomes": "current_accepted_outcome_resolutions" in relations,
            "quality": any(
                name in relations
                for name in ("current_accepted_atomic_claims", "current_accepted_claim_subjects")
            )
            or "pipeline_runs" in relations,
            "lineage": any(
                name in relations
                for name in (
                    "current_accepted_atomic_claims",
                    "current_accepted_claim_subjects",
                    "current_accepted_corpus_releases",
                )
            ),
            "accuracy": all(
                name in relations
                for name in (
                    "current_accepted_corpus_releases",
                    "current_accepted_people",
                    "current_accepted_atomic_claims",
                    "current_accepted_position_observations",
                    "current_accepted_outcome_resolutions",
                    "current_accepted_contrarian_snapshots",
                )
            ),
            "graphs": all(
                name in relations
                for name in (
                    "current_accepted_person_appearances",
                    "current_accepted_source_affiliations",
                    "current_accepted_atomic_claims",
                    "current_accepted_position_observations",
                    "current_accepted_claim_relations",
                    "current_accepted_contrarian_snapshots",
                )
            ),
        }
        return {
            "private_local_only": True,
            "read_only": bool(self.conn.execute("PRAGMA query_only").fetchone()[0]),
            "authority": "legacy_debug" if self.allow_legacy else "current_accepted_only",
            "surfaces": surfaces,
            "preferred_views": {key: name for key, name in preferred.items() if name in relations},
            "legacy_fallbacks": {
                "enabled": self.allow_legacy,
                "available": {
                    "claims": "claims" in relations,
                    "subjects": "claim_subjects" in relations,
                    "people": "canonical_people" in relations or "people" in relations,
                    "claim_edges": "agreement_edges" in relations or "disagreement_edges" in relations,
                    "outcomes": "forecast_outcome_checks" in relations,
                },
            },
        }

    def search(self, query: str, *, limit: int = 50) -> dict[str, Any]:
        query = query.strip()
        if not query:
            return self._result("search", [])
        pattern = f"%{query.lower()}%"
        size = _limit(limit)
        relations = self._relations()
        rows: list[dict[str, Any]] = []
        atomic = self._first_available("current_accepted_atomic_claims")
        if not atomic and self.allow_legacy:
            atomic = self._first_available("atomic_claims", "atomic_claim_v1")
        if atomic:
            if self.allow_legacy:
                person_select = "accepted_claim.canonical_person_id"
                person_join = ""
            elif "current_accepted_people" in relations:
                person_select = "accepted_person.id"
                person_join = (
                    "LEFT JOIN current_accepted_people AS accepted_person "
                    "ON accepted_person.id = accepted_claim.canonical_person_id"
                )
            else:
                person_select = "NULL"
                person_join = ""
            rows.extend(
                self._rows(
                    f"""
                    SELECT 'atomic_claim' AS result_type, accepted_claim.id,
                           accepted_claim.claim_text AS text, accepted_claim.claim_type,
                           accepted_claim.raw_speaker,
                           {person_select} AS canonical_person_id,
                           accepted_claim.stance, accepted_claim.certainty,
                           accepted_claim.time_horizon, accepted_claim.confidence,
                           accepted_claim.review_status, accepted_claim.observed_at,
                           accepted_claim.corpus_release_id, accepted_claim.pipeline_run_id
                    FROM {atomic} AS accepted_claim
                    {person_join}
                    WHERE lower(accepted_claim.claim_text) LIKE ?
                       OR lower(accepted_claim.raw_speaker) LIKE ?
                       OR lower(accepted_claim.claim_type) LIKE ?
                    ORDER BY accepted_claim.observed_at DESC, accepted_claim.confidence DESC
                    LIMIT ?
                    """,
                    (pattern, pattern, pattern, size),
                )
            )
        if self.allow_legacy and "claims" in relations and len(rows) < size:
            rows.extend(
                self._rows(
                    """
                    SELECT 'legacy_claim' AS result_type, id, text, stance,
                           confidence, segment_id, label_id, created_at
                    FROM claims
                    WHERE lower(text) LIKE ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (pattern, size - len(rows)),
                )
            )
        if self.allow_legacy and "claim_subjects" in relations and len(rows) < size:
            rows.extend(
                self._rows(
                    """
                    SELECT 'subject' AS result_type, id, subject_text AS text,
                           domain, subject_type, status, confidence, method, updated_at
                    FROM claim_subjects
                    WHERE lower(subject_text) LIKE ? OR lower(COALESCE(domain, '')) LIKE ?
                    ORDER BY confidence DESC, updated_at DESC
                    LIMIT ?
                    """,
                    (pattern, pattern, size - len(rows)),
                )
            )
        if "current_accepted_claim_subjects" in relations and len(rows) < size:
            rows.extend(
                self._rows(
                    """
                    SELECT 'subject' AS result_type, id, subject_text AS text,
                           subject_type, domain, confidence, review_status,
                           decided_at, corpus_release_id, pipeline_run_id
                    FROM current_accepted_claim_subjects
                    WHERE lower(subject_text) LIKE ?
                       OR lower(COALESCE(domain, '')) LIKE ?
                       OR lower(subject_type) LIKE ?
                    ORDER BY decided_at DESC, confidence DESC
                    LIMIT ?
                    """,
                    (pattern, pattern, pattern, size - len(rows)),
                )
            )
        if len(rows) < size:
            people = self.people(query=query, limit=size - len(rows)).get("data") or []
            rows.extend(
                {
                    "result_type": "person",
                    "id": item.get("id"),
                    "text": item.get("display_name"),
                    **{key: value for key, value in item.items() if key not in {"id", "display_name"}},
                }
                for item in people
            )
        return self._result("search", rows[:size], query=query)

    def subjects(self, *, query: str | None = None, limit: int = 50) -> dict[str, Any]:
        relations = self._relations()
        pattern = f"%{(query or '').strip().lower()}%"
        if "current_accepted_claim_subjects" in relations:
            variant_count = (
                "(SELECT COUNT(*) FROM current_accepted_proposition_variants AS variants "
                "WHERE variants.subject_id = subjects.id)"
                if "current_accepted_proposition_variants" in relations
                else "0"
            )
            position_count = (
                "(SELECT COUNT(*) FROM current_accepted_position_observations AS positions "
                "WHERE positions.subject_id = subjects.id)"
                if "current_accepted_position_observations" in relations
                else "0"
            )
            people_count = (
                "(SELECT COUNT(DISTINCT canonical_person_id) "
                "FROM current_accepted_position_observations AS positions "
                "WHERE positions.subject_id = subjects.id)"
                if "current_accepted_position_observations" in relations
                else "0"
            )
            claim_count = (
                "(SELECT COUNT(DISTINCT atomic_claim_id) "
                "FROM current_accepted_position_observations AS positions "
                "WHERE positions.subject_id = subjects.id)"
                if "current_accepted_position_observations" in relations
                else "0"
            )
            rows = self._rows(
                f"""
                SELECT subjects.*, {variant_count} AS variant_count,
                       {position_count} AS position_count,
                       {people_count} AS resolved_people_count,
                       {claim_count} AS claim_count
                FROM current_accepted_claim_subjects AS subjects
                WHERE ? = '%%'
                   OR lower(subjects.subject_text) LIKE ?
                   OR lower(COALESCE(subjects.domain, '')) LIKE ?
                   OR lower(subjects.subject_type) LIKE ?
                ORDER BY position_count DESC, claim_count DESC,
                         subjects.confidence DESC, subjects.decided_at DESC
                LIMIT ?
                """,
                (pattern, pattern, pattern, pattern, _limit(limit)),
            )
            if rows or not self.allow_legacy:
                return self._result("subjects", rows, query=query)
        if not self.allow_legacy:
            return self._result(
                "subjects",
                [],
                unavailable="no versioned current-accepted subject surface is installed",
            )
        if "claim_subjects" not in relations:
            return self._result("subjects", [], unavailable="legacy claim_subjects schema is not installed")
        rows = self._rows(
            """
            WITH member_counts AS (
              SELECT subject_id, COUNT(DISTINCT claim_id) AS claim_count
              FROM claim_subject_members
              WHERE relation = 'about_subject'
              GROUP BY subject_id
            ), variant_counts AS (
              SELECT subject_id, COUNT(*) AS variant_count
              FROM claim_proposition_variants
              GROUP BY subject_id
            ), position_counts AS (
              SELECT subject_id, COUNT(*) AS position_count,
                     COUNT(DISTINCT canonical_person_id) AS resolved_people_count,
                     COUNT(DISTINCT source_id) AS source_count
              FROM claim_position_observations
              GROUP BY subject_id
            ), event_counts AS (
              SELECT subject_id, COUNT(*) AS event_count
              FROM claim_subject_event_observations
              GROUP BY subject_id
            )
            SELECT claim_subjects.id, claim_subjects.subject_text, claim_subjects.domain,
                   claim_subjects.subject_type, claim_subjects.status, claim_subjects.confidence,
                   claim_subjects.method, claim_subjects.created_at, claim_subjects.updated_at,
                   COALESCE(member_counts.claim_count, 0) AS claim_count,
                   COALESCE(variant_counts.variant_count, 0) AS variant_count,
                   COALESCE(position_counts.position_count, 0) AS position_count,
                   COALESCE(position_counts.resolved_people_count, 0) AS resolved_people_count,
                   COALESCE(position_counts.source_count, 0) AS source_count,
                   COALESCE(event_counts.event_count, 0) AS event_count
            FROM claim_subjects
            LEFT JOIN member_counts ON member_counts.subject_id = claim_subjects.id
            LEFT JOIN variant_counts ON variant_counts.subject_id = claim_subjects.id
            LEFT JOIN position_counts ON position_counts.subject_id = claim_subjects.id
            LEFT JOIN event_counts ON event_counts.subject_id = claim_subjects.id
            WHERE ? = '%%'
               OR lower(claim_subjects.subject_text) LIKE ?
               OR lower(COALESCE(claim_subjects.domain, '')) LIKE ?
            ORDER BY claim_count DESC, position_count DESC, claim_subjects.confidence DESC
            LIMIT ?
            """,
            (pattern, pattern, pattern, _limit(limit)),
        )
        return self._result("subjects", rows, query=query)

    def people(self, *, query: str | None = None, limit: int = 50) -> dict[str, Any]:
        relations = self._relations()
        pattern = f"%{(query or '').strip().lower()}%"
        size = _limit(limit)
        accepted_refs: list[str] = []
        if "current_accepted_person_appearances" in relations:
            accepted_refs.append(
                "SELECT canonical_person_id, source_id, appeared_at AS observed_at FROM current_accepted_person_appearances"
            )
        if "current_accepted_atomic_claims" in relations:
            accepted_refs.append(
                "SELECT canonical_person_id, source_id, observed_at FROM current_accepted_atomic_claims WHERE canonical_person_id IS NOT NULL"
            )
        if "current_accepted_position_observations" in relations:
            accepted_refs.append(
                "SELECT canonical_person_id, NULL AS source_id, observed_at "
                "FROM current_accepted_position_observations"
            )
        if "current_accepted_people" in relations and accepted_refs and not self.allow_legacy:
            references = " UNION ALL ".join(accepted_refs)
            rows = self._rows(
                f"""
                WITH accepted_person_references AS (
                  {references}
                ), accepted_people AS (
                  SELECT canonical_person_id, COUNT(*) AS appearance_count,
                         COUNT(DISTINCT source_id) AS source_count,
                         MAX(observed_at) AS last_appeared_at
                  FROM accepted_person_references
                  WHERE canonical_person_id IS NOT NULL
                  GROUP BY canonical_person_id
                )
                SELECT accepted_identities.id, accepted_identities.display_name,
                       accepted_identities.primary_org_id, accepted_identities.confidence,
                       accepted_identities.status, accepted_identities.canonical_version,
                       accepted_identities.created_at, accepted_identities.updated_at,
                       accepted_people.appearance_count, accepted_people.source_count,
                       accepted_people.last_appeared_at
                FROM current_accepted_people AS accepted_identities
                JOIN accepted_people ON accepted_people.canonical_person_id = accepted_identities.id
                WHERE (
                    ? = '%%'
                    OR lower(accepted_identities.display_name) LIKE ?
                    OR lower(accepted_identities.normalized_name) LIKE ?
                  )
                ORDER BY appearance_count DESC, accepted_identities.confidence DESC, accepted_identities.display_name
                LIMIT ?
                """,
                (pattern, pattern, pattern, size),
            )
            return self._result("people", rows, query=query)
        if self.allow_legacy and "canonical_people" in relations:
            rows = self._rows(
                """
                SELECT id, display_name, primary_org_id, confidence, status,
                       canonical_version, created_at, updated_at,
                       0 AS appearance_count, 0 AS source_count, NULL AS last_appeared_at
                FROM canonical_people
                WHERE ? = '%%' OR lower(display_name) LIKE ? OR lower(normalized_name) LIKE ?
                ORDER BY confidence DESC, display_name
                LIMIT ?
                """,
                (pattern, pattern, pattern, size),
            )
            return self._result("people", rows, query=query)
        if self.allow_legacy and "people" in relations:
            rows = self._rows(
                """
                SELECT id, name AS display_name, org_id AS primary_org_id, created_at, updated_at
                FROM people
                WHERE ? = '%%' OR lower(name) LIKE ?
                ORDER BY name
                LIMIT ?
                """,
                (pattern, pattern, size),
            )
            return self._result("people", rows, query=query)
        return self._result(
            "people",
            [],
            unavailable="no people are referenced by a current accepted intelligence surface",
        )

    def networks(
        self,
        *,
        person_id: str | None = None,
        source_id: str | None = None,
        claim_id: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        size = _limit(limit)
        data: dict[str, list[dict[str, Any]]] = {}
        available = self._relations()
        positions = self._first_available("current_accepted_position_observations")
        if positions:
            filters: list[str] = []
            params: list[Any] = []
            if person_id:
                filters.append("positions.canonical_person_id = ?")
                params.append(person_id)
            if claim_id:
                filters.append("positions.atomic_claim_id = ?")
                params.append(claim_id)
            where = "WHERE " + " AND ".join(filters) if filters else ""
            subject_join = (
                "LEFT JOIN current_accepted_claim_subjects AS subjects "
                "ON subjects.id = positions.subject_id"
                if "current_accepted_claim_subjects" in available
                else ""
            )
            subject_select = "subjects.subject_text" if subject_join else "NULL AS subject_text"
            variant_join = (
                "LEFT JOIN current_accepted_proposition_variants AS variants "
                "ON variants.id = positions.variant_id"
                if "current_accepted_proposition_variants" in available
                else ""
            )
            variant_select = "variants.proposition_text" if variant_join else "NULL AS proposition_text"
            claim_join = (
                "LEFT JOIN current_accepted_atomic_claims AS claims "
                "ON claims.id = positions.atomic_claim_id"
                if "current_accepted_atomic_claims" in available
                else ""
            )
            claim_select = "claims.claim_text" if claim_join else "NULL AS claim_text"
            person_join = (
                "LEFT JOIN current_accepted_people AS people "
                "ON people.id = positions.canonical_person_id"
                if "current_accepted_people" in available
                else ""
            )
            person_select = "people.display_name" if person_join else "NULL AS display_name"
            data["positions"] = self._rows(
                f"""
                SELECT positions.*, {subject_select}, {variant_select},
                       {claim_select}, {person_select}
                FROM {positions} AS positions
                {subject_join}
                {variant_join}
                {claim_join}
                {person_join}
                {where}
                ORDER BY positions.observed_at DESC, positions.confidence DESC
                LIMIT ?
                """,
                (*params, size),
            )
        appearances = self._first_available("current_accepted_person_appearances")
        if not appearances and self.allow_legacy:
            appearances = self._first_available("person_appearances")
        if appearances:
            where, params = self._where(
                (("person_appearances.canonical_person_id", person_id), ("person_appearances.source_id", source_id))
            )
            if not self.allow_legacy and "current_accepted_people" in available:
                accepted_people_join = (
                    "LEFT JOIN current_accepted_people AS accepted_people "
                    "ON accepted_people.id = person_appearances.canonical_person_id"
                )
                accepted_people_select = "accepted_people.display_name"
            elif self.allow_legacy and "canonical_people" in available:
                accepted_people_join = (
                    "LEFT JOIN canonical_people AS accepted_people "
                    "ON accepted_people.id = person_appearances.canonical_person_id"
                )
                accepted_people_select = "accepted_people.display_name"
            else:
                accepted_people_join = ""
                accepted_people_select = "NULL AS display_name"
            data["appearances"] = self._rows(
                f"""
                SELECT person_appearances.id, person_appearances.canonical_person_id,
                       {accepted_people_select}, person_appearances.source_id,
                       sources.name AS source_name, person_appearances.episode_id,
                       episodes.title AS episode_title, person_appearances.role,
                       person_appearances.appeared_at, person_appearances.confidence,
                       person_appearances.review_status
                FROM {appearances} AS person_appearances
                {accepted_people_join}
                LEFT JOIN sources ON sources.id = person_appearances.source_id
                LEFT JOIN episodes ON episodes.id = person_appearances.episode_id
                {where}
                ORDER BY person_appearances.appeared_at DESC
                LIMIT ?
                """,
                (*params, size),
            )
        affiliations = self._first_available("current_accepted_source_affiliations")
        if not affiliations and self.allow_legacy:
            affiliations = self._first_available("source_affiliations")
        if affiliations:
            where, params = self._where((("source_affiliations.source_id", source_id),))
            data["source_affiliations"] = self._rows(
                f"""
                SELECT source_affiliations.id, source_affiliations.source_id,
                       sources.name AS source_name, source_affiliations.affiliation_kind,
                       source_affiliations.affiliation_key, source_affiliations.affiliation_name,
                       source_affiliations.canonical_org_id, source_affiliations.valid_from,
                       source_affiliations.valid_to, source_affiliations.confidence,
                       source_affiliations.review_status
                FROM {affiliations} AS source_affiliations
                LEFT JOIN sources ON sources.id = source_affiliations.source_id
                {where}
                ORDER BY source_affiliations.valid_from DESC, source_affiliations.confidence DESC
                LIMIT ?
                """,
                (*params, size),
            )
        relations = self._first_available("current_accepted_claim_relations")
        if not relations and self.allow_legacy:
            relations = self._first_available("claim_relation_judgments")
        if relations:
            filters: list[str] = []
            params: list[Any] = []
            if claim_id:
                filters.append("(claim_relations.source_claim_id = ? OR claim_relations.target_claim_id = ?)")
                params.extend([claim_id, claim_id])
            where = "WHERE " + " AND ".join(filters) if filters else ""
            atomic_surface = self._first_available("current_accepted_atomic_claims")
            if not atomic_surface and self.allow_legacy:
                atomic_surface = self._first_available("atomic_claims")
            if not atomic_surface:
                return self._result(
                    "networks",
                    data,
                    person_id=person_id,
                    source_id=source_id,
                    claim_id=claim_id,
                    unavailable="claim relations exist without an accepted atomic-claim surface",
                )
            data["claim_relations"] = self._rows(
                f"""
                SELECT claim_relations.id, claim_relations.source_claim_id,
                       source_claim.claim_text AS source_claim_text,
                       claim_relations.target_claim_id,
                       target_claim.claim_text AS target_claim_text,
                       claim_relations.relation, claim_relations.temporal_scope,
                       claim_relations.confidence,
                       claim_relations.rationale, claim_relations.judge_model,
                       claim_relations.schema_version,
                       claim_relations.corpus_release_id,
                       claim_relations.pipeline_run_id,
                       claim_relations.review_status,
                       claim_relations.decided_at
                FROM {relations} AS claim_relations
                LEFT JOIN {atomic_surface} AS source_claim ON source_claim.id = claim_relations.source_claim_id
                LEFT JOIN {atomic_surface} AS target_claim ON target_claim.id = claim_relations.target_claim_id
                {where}
                ORDER BY claim_relations.decided_at DESC, claim_relations.confidence DESC
                LIMIT ?
                """,
                (*params, size),
            )
        if self.allow_legacy:
            for name, sql in self._legacy_network_queries(person_id=person_id, limit=size).items():
                if name in self._relations():
                    data[name] = self._rows(sql[0], sql[1])
        return self._result("networks", data, person_id=person_id, source_id=source_id, claim_id=claim_id)

    def consensus(self, *, claim_id: str | None = None, limit: int = 50) -> dict[str, Any]:
        size = _limit(limit)
        data: dict[str, list[dict[str, Any]]] = {}
        consensus = self._first_available("current_accepted_consensus_snapshots")
        if not consensus and self.allow_legacy:
            consensus = self._first_available("consensus_snapshots")
        if consensus:
            clause = "WHERE consensus.focal_claim_id = ?" if claim_id else ""
            params: tuple[Any, ...] = (claim_id,) if claim_id else ()
            atomic_surface = self._first_available("current_accepted_atomic_claims")
            if not atomic_surface and self.allow_legacy:
                atomic_surface = self._first_available("atomic_claims")
            if not atomic_surface:
                return self._result("consensus", {}, claim_id=claim_id, unavailable="accepted atomic claims are unavailable")
            data["consensus"] = self._rows(
                f"""
                SELECT consensus.*, accepted_claims.claim_text AS focal_claim_text
                FROM {consensus} AS consensus
                LEFT JOIN {atomic_surface} AS accepted_claims ON accepted_claims.id = consensus.focal_claim_id
                {clause}
                ORDER BY consensus.as_of DESC
                LIMIT ?
                """,
                (*params, size),
            )
        contrarian = self._first_available("current_accepted_contrarian_snapshots")
        if not contrarian and self.allow_legacy:
            contrarian = self._first_available("contrarian_snapshots")
        if contrarian:
            clause = "WHERE contrarian.target_claim_id = ?" if claim_id else ""
            params = (claim_id,) if claim_id else ()
            atomic_surface = self._first_available("current_accepted_atomic_claims")
            if not atomic_surface and self.allow_legacy:
                atomic_surface = self._first_available("atomic_claims")
            if not atomic_surface:
                return self._result("consensus", data, claim_id=claim_id, unavailable="accepted atomic claims are unavailable")
            data["contrarian"] = self._rows(
                f"""
                SELECT contrarian.*, accepted_claims.claim_text AS target_claim_text
                FROM {contrarian} AS contrarian
                LEFT JOIN {atomic_surface} AS accepted_claims ON accepted_claims.id = contrarian.target_claim_id
                {clause}
                ORDER BY contrarian.as_of DESC
                LIMIT ?
                """,
                (*params, size),
            )
        relation_result = self.networks(claim_id=claim_id, limit=size)["data"]
        if relation_result.get("claim_relations"):
            data["claim_relations"] = relation_result["claim_relations"]
        if self.allow_legacy and not any(data.values()):
            data.update(self._legacy_consensus(claim_id=claim_id, limit=size))
        return self._result("consensus", data, claim_id=claim_id)

    def accuracy_history(
        self,
        person_id: str,
        *,
        as_of: str | None = None,
        observed_from: str | None = None,
        observed_through: str | None = None,
        minimum_resolved_claims: int = 10,
    ) -> dict[str, Any]:
        if not str(person_id or "").strip():
            raise ValueError("person_id is required")
        try:
            data = person_accuracy_history(
                self.conn,
                str(person_id),
                as_of=as_of,
                observed_from=observed_from,
                observed_through=observed_through,
                minimum_resolved_claims=minimum_resolved_claims,
            )
        except (AccuracyUnavailableError, AccuracyDataError) as exc:
            return self._result("accuracy_history", {}, unavailable=str(exc))
        return self._result("accuracy_history", data)

    def accuracy_rankings(
        self,
        *,
        as_of: str | None = None,
        observed_from: str | None = None,
        observed_through: str | None = None,
        minimum_resolved_claims: int = 10,
        limit: int = 50,
    ) -> dict[str, Any]:
        try:
            data = person_accuracy_rankings(
                self.conn,
                as_of=as_of,
                observed_from=observed_from,
                observed_through=observed_through,
                minimum_resolved_claims=minimum_resolved_claims,
            )
        except (AccuracyUnavailableError, AccuracyDataError) as exc:
            return self._result("accuracy_rankings", {}, unavailable=str(exc))
        bounded = dict(data)
        rankings = list(data.get("rankings") or [])
        bounded["returned_ranking_count"] = min(len(rankings), _limit(limit))
        bounded["rankings"] = rankings[: _limit(limit)]
        return self._result("accuracy_rankings", bounded)

    def contrarian_success(
        self,
        person_id: str,
        *,
        as_of: str | None = None,
        observed_from: str | None = None,
        observed_through: str | None = None,
    ) -> dict[str, Any]:
        if not str(person_id or "").strip():
            raise ValueError("person_id is required")
        try:
            data = person_contrarian_success(
                self.conn,
                str(person_id),
                as_of=as_of,
                observed_from=observed_from,
                observed_through=observed_through,
            )
        except (AccuracyUnavailableError, AccuracyDataError) as exc:
            return self._result("contrarian_success", {}, unavailable=str(exc))
        return self._result("contrarian_success", data)

    def graphs(self, *, as_of: str | None = None, limit: int = 50) -> dict[str, Any]:
        data = accepted_graph_bundle(
            self.conn,
            as_of=as_of or now_iso(),
            limit=_limit(limit),
        )
        return self._result("graphs", data)

    def outcomes(self, *, claim_id: str | None = None, person_id: str | None = None, limit: int = 50) -> dict[str, Any]:
        size = _limit(limit)
        outcomes = self._first_available("current_accepted_outcome_resolutions")
        if not outcomes and self.allow_legacy:
            outcomes = self._first_available("outcome_resolution_revisions")
        if outcomes:
            filters: list[str] = []
            params: list[Any] = []
            if claim_id:
                filters.append("outcomes.claim_id = ?")
                params.append(claim_id)
            atomic_surface = self._first_available("current_accepted_atomic_claims")
            if not atomic_surface and self.allow_legacy:
                atomic_surface = self._first_available("atomic_claims")
            if not atomic_surface:
                return self._result(
                    "outcomes",
                    [],
                    claim_id=claim_id,
                    person_id=person_id,
                    unavailable="accepted atomic claims are unavailable",
                )
            if self.allow_legacy:
                person_select = "accepted_claims.canonical_person_id"
                person_join = ""
                if person_id:
                    filters.append("accepted_claims.canonical_person_id = ?")
                    params.append(person_id)
            elif "current_accepted_people" in self._relations():
                person_select = "accepted_person.id"
                person_join = (
                    "LEFT JOIN current_accepted_people AS accepted_person "
                    "ON accepted_person.id = accepted_claims.canonical_person_id"
                )
                if person_id:
                    filters.append("accepted_person.id = ?")
                    params.append(person_id)
            else:
                person_select = "NULL"
                person_join = ""
                if person_id:
                    return self._result("outcomes", [], claim_id=claim_id, person_id=person_id)
            where = "WHERE " + " AND ".join(filters) if filters else ""
            rows = self._rows(
                f"""
                SELECT outcomes.*, accepted_claims.claim_text,
                       accepted_claims.raw_speaker,
                       {person_select} AS canonical_person_id,
                       accepted_claims.forecast_probability, accepted_claims.time_horizon
                FROM {outcomes} AS outcomes
                JOIN {atomic_surface} AS accepted_claims ON accepted_claims.id = outcomes.claim_id
                {person_join}
                {where}
                ORDER BY outcomes.resolved_at DESC, outcomes.revision DESC
                LIMIT ?
                """,
                (*params, size),
            )
            if rows or not self.allow_legacy:
                return self._result("outcomes", rows, claim_id=claim_id, person_id=person_id)
        if self.allow_legacy and "forecast_outcome_checks" in self._relations():
            filters = []
            params = []
            if claim_id:
                filters.append("forecast_outcome_checks.claim_id = ?")
                params.append(claim_id)
            if person_id:
                filters.append("forecast_outcome_checks.canonical_person_id = ?")
                params.append(person_id)
            where = "WHERE " + " AND ".join(filters) if filters else ""
            rows = self._rows(
                f"""
                SELECT forecast_outcome_checks.*, claims.text AS claim_text
                FROM forecast_outcome_checks
                JOIN claims ON claims.id = forecast_outcome_checks.claim_id
                {where}
                ORDER BY forecast_outcome_checks.updated_at DESC
                LIMIT ?
                """,
                (*params, size),
            )
            return self._result("outcomes", rows, claim_id=claim_id, person_id=person_id)
        return self._result("outcomes", [], unavailable="outcome schema is not installed")

    def quality(self, *, limit: int = 50) -> dict[str, Any]:
        size = _limit(limit)
        relations = self._relations()
        data: dict[str, Any] = {}
        if self.allow_legacy and "quality_audits" in relations:
            data["quality_audits"] = self._rows(
                """
                SELECT label_pack, status, COUNT(*) AS count, AVG(score) AS average_score,
                       MAX(created_at) AS last_observed_at
                FROM quality_audits
                GROUP BY label_pack, status
                ORDER BY last_observed_at DESC
                LIMIT ?
                """,
                (size,),
            )
        if "current_accepted_claim_subjects" in relations:
            data["accepted_subjects"] = self._rows(
                """
                SELECT judge_schema_version, judge_model, subject_type, review_status,
                       COUNT(*) AS count, AVG(confidence) AS average_confidence,
                       MIN(decided_at) AS first_decided_at,
                       MAX(decided_at) AS last_decided_at
                FROM current_accepted_claim_subjects
                GROUP BY judge_schema_version, judge_model, subject_type, review_status
                ORDER BY count DESC, judge_schema_version, judge_model, subject_type
                LIMIT ?
                """,
                (size,),
            )
        if self.allow_legacy and "reviewer_audits" in relations:
            data["reviewer_audits"] = self._rows(
                """
                SELECT model, review_mode, status, COUNT(*) AS count,
                       AVG(overall_score) AS overall_score,
                       AVG(coverage_score) AS coverage_score,
                       AVG(precision_score) AS precision_score,
                       AVG(grounding_score) AS grounding_score,
                       SUM(p0_issue_count) AS p0_issue_count,
                       MAX(updated_at) AS last_observed_at
                FROM reviewer_audits
                GROUP BY model, review_mode, status
                ORDER BY last_observed_at DESC
                LIMIT ?
                """,
                (size,),
            )
        if "current_accepted_atomic_claims" in relations:
            data["accepted_claims"] = self._rows(
                """
                SELECT extractor_schema, extractor_schema_version, extractor_model,
                       COUNT(*) AS count, AVG(confidence) AS average_confidence,
                       MIN(observed_at) AS first_observed_at,
                       MAX(observed_at) AS last_observed_at
                FROM current_accepted_atomic_claims
                GROUP BY extractor_schema, extractor_schema_version, extractor_model
                ORDER BY count DESC, extractor_schema, extractor_schema_version
                LIMIT ?
                """,
                (size,),
            )
        if "pipeline_runs" in relations:
            accepted_runs = self._accepted_pipeline_run_subquery()
            if self.allow_legacy:
                status_filter = ""
            elif accepted_runs:
                status_filter = f"WHERE id IN ({accepted_runs}) AND status = 'succeeded'"
            else:
                status_filter = "WHERE 0"
            data["pipeline_runs"] = self._rows(
                f"""
                SELECT run_type, run_schema, run_schema_version, model, status,
                       COUNT(*) AS count, MAX(updated_at) AS last_observed_at
                FROM pipeline_runs
                {status_filter}
                GROUP BY run_type, run_schema, run_schema_version, model, status
                ORDER BY last_observed_at DESC
                LIMIT ?
                """,
                (size,),
            )
        return self._result("quality", data)

    def lineage(self, identifier: str, *, kind: str = "claim", limit: int = 100) -> dict[str, Any]:
        size = _limit(limit, 100)
        if kind in {"claim", "atomic_claim"}:
            atomic = self._first_available("current_accepted_atomic_claims")
            if not atomic and self.allow_legacy:
                atomic = self._first_available("atomic_claims", "atomic_claim_v1")
            if atomic and self.conn.execute(f"SELECT 1 FROM {atomic} WHERE id = ?", (identifier,)).fetchone():
                person_select = ", canonical_people.display_name AS canonical_person_name" if self.allow_legacy else ""
                person_join = (
                    "LEFT JOIN canonical_people ON canonical_people.id = atomic_claims.canonical_person_id"
                    if self.allow_legacy
                    else ""
                )
                claim = self._rows(
                    f"""
                    SELECT atomic_claims.*{person_select},
                           sources.name AS source_name, episodes.title AS episode_title
                    FROM {atomic} AS atomic_claims
                    {person_join}
                    LEFT JOIN sources ON sources.id = atomic_claims.source_id
                    LEFT JOIN episodes ON episodes.id = atomic_claims.episode_id
                    WHERE atomic_claims.id = ?
                    """,
                    (identifier,),
                )[0]
                data: dict[str, Any] = {"claim": claim, "revisions": [], "pipeline_run": [], "corpus_release": []}
                if not self.allow_legacy:
                    claim = self._redact_candidate_person(claim)
                    data["claim"] = claim
                if "claim_lineage_id" in claim:
                    revision_surface = (
                        self._first_available("atomic_claims", "atomic_claim_v1")
                        if self.allow_legacy
                        else atomic
                    )
                    if revision_surface:
                        data["revisions"] = self._rows(
                            f"SELECT * FROM {revision_surface} WHERE claim_lineage_id = ? ORDER BY revision DESC LIMIT ?",
                            (claim["claim_lineage_id"], size),
                        )
                        if not self.allow_legacy:
                            data["revisions"] = [
                                self._redact_candidate_person(revision)
                                for revision in data["revisions"]
                            ]
                if claim.get("pipeline_run_id") and "pipeline_runs" in self._relations():
                    status_filter = "" if self.allow_legacy else " AND status = 'succeeded'"
                    data["pipeline_run"] = self._rows(
                        f"SELECT * FROM pipeline_runs WHERE id = ?{status_filter}",
                        (claim["pipeline_run_id"],),
                    )
                if claim.get("corpus_release_id") and "corpus_releases" in self._relations():
                    release_surface = "corpus_releases" if self.allow_legacy else "current_accepted_corpus_releases"
                    if release_surface in self._relations():
                        data["corpus_release"] = self._rows(
                            f"SELECT * FROM {release_surface} WHERE id = ?",
                            (claim["corpus_release_id"],),
                        )
                return self._result("lineage", data, identifier=identifier, kind=kind)
            if self.allow_legacy and "claims" in self._relations():
                rows = self._rows(
                    """
                    SELECT claims.id, claims.text, claims.stance, claims.confidence,
                           claims.evidence_json, claims.created_at, labels.id AS label_id,
                           labels.label_pack, labels.label_pack_version, labels.model,
                           labels.status AS label_status, segments.id AS segment_id,
                           segments.transcript_id, episodes.id AS episode_id,
                           episodes.title AS episode_title, episodes.published_at,
                           sources.id AS source_id, sources.name AS source_name
                    FROM claims
                    JOIN labels ON labels.id = claims.label_id
                    JOIN segments ON segments.id = claims.segment_id
                    JOIN episodes ON episodes.id = segments.episode_id
                    JOIN sources ON sources.id = segments.source_id
                    WHERE claims.id = ?
                    """,
                    (identifier,),
                )
                return self._result("lineage", rows[0] if rows else {}, identifier=identifier, kind=kind)
        if kind == "subject" and "current_accepted_claim_subjects" in self._relations():
            subject = self._rows(
                "SELECT * FROM current_accepted_claim_subjects WHERE id = ?",
                (identifier,),
            )
            if subject:
                variants = (
                    self._rows(
                        """
                        SELECT * FROM current_accepted_proposition_variants
                        WHERE subject_id = ?
                        ORDER BY confidence DESC, decided_at DESC
                        LIMIT ?
                        """,
                        (identifier, size),
                    )
                    if "current_accepted_proposition_variants" in self._relations()
                    else []
                )
                positions = (
                    self._rows(
                        """
                        SELECT positions.*, claims.claim_text,
                               people.display_name AS canonical_person_name
                        FROM current_accepted_position_observations AS positions
                        JOIN current_accepted_atomic_claims AS claims
                          ON claims.id = positions.atomic_claim_id
                        JOIN current_accepted_people AS people
                          ON people.id = positions.canonical_person_id
                        WHERE positions.subject_id = ?
                        ORDER BY positions.observed_at DESC, positions.confidence DESC
                        LIMIT ?
                        """,
                        (identifier, size),
                    )
                    if {
                        "current_accepted_position_observations",
                        "current_accepted_atomic_claims",
                        "current_accepted_people",
                    }.issubset(self._relations())
                    else []
                )
                return self._result(
                    "lineage",
                    {"subject": subject[0], "variants": variants, "positions": positions},
                    identifier=identifier,
                    kind=kind,
                )
        if self.allow_legacy and kind == "subject" and "claim_subjects" in self._relations():
            subject = self._rows("SELECT * FROM claim_subjects WHERE id = ?", (identifier,))
            members = self._rows(
                """
                SELECT claim_subject_members.*, claims.text AS claim_text, claims.stance
                FROM claim_subject_members
                JOIN claims ON claims.id = claim_subject_members.claim_id
                WHERE claim_subject_members.subject_id = ?
                ORDER BY claim_subject_members.confidence DESC
                LIMIT ?
                """,
                (identifier, size),
            )
            variants = self._rows(
                "SELECT * FROM claim_proposition_variants WHERE subject_id = ? ORDER BY confidence DESC LIMIT ?",
                (identifier, size),
            )
            return self._result("lineage", {"subject": subject, "members": members, "variants": variants}, identifier=identifier, kind=kind)
        if kind == "pipeline_run" and "pipeline_runs" in self._relations():
            accepted_runs = self._accepted_pipeline_run_subquery()
            if self.allow_legacy:
                status_filter = ""
            elif accepted_runs:
                status_filter = f" AND id IN ({accepted_runs}) AND status = 'succeeded'"
            else:
                status_filter = " AND 0"
            return self._result(
                "lineage",
                self._rows(f"SELECT * FROM pipeline_runs WHERE id = ?{status_filter}", (identifier,)),
                identifier=identifier,
                kind=kind,
            )
        if kind == "corpus_release":
            release_surface = "corpus_releases" if self.allow_legacy else "current_accepted_corpus_releases"
            if release_surface in self._relations():
                return self._result(
                    "lineage",
                    self._rows(f"SELECT * FROM {release_surface} WHERE id = ?", (identifier,)),
                    identifier=identifier,
                    kind=kind,
                )
        return self._result("lineage", {}, identifier=identifier, kind=kind, unavailable="identifier or schema surface was not found")

    def _legacy_network_queries(self, *, person_id: str | None, limit: int) -> dict[str, tuple[str, tuple[Any, ...]]]:
        filters = "WHERE canonical_person_id = ?" if person_id else ""
        params: tuple[Any, ...] = (person_id, limit) if person_id else (limit,)
        mention_filter = "WHERE source_person_id = ? OR target_person_id = ?" if person_id else ""
        mention_params: tuple[Any, ...] = (person_id, person_id, limit) if person_id else (limit,)
        return {
            "person_concept_edges": (
                f"SELECT * FROM person_concept_edges {filters} ORDER BY weight DESC, confidence DESC LIMIT ?",
                params,
            ),
            "person_org_edges": (
                f"SELECT * FROM person_org_edges {filters} ORDER BY confidence DESC LIMIT ?",
                params,
            ),
            "person_person_mentions": (
                f"SELECT * FROM person_person_mentions {mention_filter} ORDER BY created_at DESC LIMIT ?",
                mention_params,
            ),
        }

    def _legacy_consensus(self, *, claim_id: str | None, limit: int) -> dict[str, list[dict[str, Any]]]:
        data: dict[str, list[dict[str, Any]]] = {}
        for table in ("agreement_edges", "disagreement_edges"):
            if table not in self._relations():
                continue
            if claim_id:
                where = "WHERE source_claim_id = ? OR target_claim_id = ?"
                params: tuple[Any, ...] = (claim_id, claim_id, limit)
            else:
                where = ""
                params = (limit,)
            data[table] = self._rows(
                f"""
                SELECT {table}.*, source_claim.text AS source_claim_text,
                       target_claim.text AS target_claim_text
                FROM {table}
                JOIN claims AS source_claim ON source_claim.id = {table}.source_claim_id
                JOIN claims AS target_claim ON target_claim.id = {table}.target_claim_id
                {where}
                ORDER BY {table}.confidence DESC, {table}.created_at DESC
                LIMIT ?
                """,
                params,
            )
        return data

    def _relations(self) -> set[str]:
        return {
            str(row["name"])
            for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            ).fetchall()
        }

    def _accepted_pipeline_run_subquery(self) -> str:
        relations = self._relations()
        parts = [
            f"SELECT pipeline_run_id AS id FROM {surface}"
            for surface in (
                "current_accepted_atomic_claims",
                "current_accepted_claim_subjects",
                "current_accepted_proposition_variants",
                "current_accepted_position_observations",
                "current_accepted_identity_resolutions",
                "current_accepted_source_affiliations",
                "current_accepted_person_appearances",
                "current_accepted_claim_relations",
                "current_accepted_outcome_resolutions",
                "current_accepted_consensus_snapshots",
                "current_accepted_contrarian_snapshots",
            )
            if surface in relations
        ]
        if "current_accepted_corpus_releases" in relations:
            parts.append(
                "SELECT promotion_pipeline_run_id AS id "
                "FROM current_accepted_corpus_releases"
            )
        return " UNION ".join(parts)

    def _redact_candidate_person(self, row: dict[str, Any]) -> dict[str, Any]:
        person_id = row.get("canonical_person_id")
        relations = self._relations()
        if not person_id or "current_accepted_people" not in relations:
            row["canonical_person_id"] = None
            return row
        accepted = self.conn.execute(
            "SELECT 1 FROM current_accepted_people WHERE id = ?",
            (person_id,),
        ).fetchone()
        if not accepted:
            row["canonical_person_id"] = None
        return row

    def _first_available(self, *names: str) -> str | None:
        relations = self._relations()
        return next((name for name in names if name in relations), None)

    def _rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    @staticmethod
    def _where(filters: tuple[tuple[str, str | None], ...]) -> tuple[str, tuple[Any, ...]]:
        selected = [(column, value) for column, value in filters if value]
        if not selected:
            return "", ()
        return "WHERE " + " AND ".join(f"{column} = ?" for column, _ in selected), tuple(value for _, value in selected)

    def _result(self, surface: str, data: Any, **metadata: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "surface": surface,
            "privacy": "local_private_read_only",
            "data": data,
            **metadata,
        }


class QueryHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], database_path: Path):
        super().__init__(server_address, QueryHandler)
        self.database_path = database_path


class QueryHandler(BaseHTTPRequestHandler):
    server: QueryHTTPServer

    def do_HEAD(self) -> None:
        route = urlsplit(self.path).path
        if route in {
            "/healthz",
            "/v1/capabilities",
            "/v1/search",
            "/v1/subjects",
            "/v1/people",
            "/v1/networks",
            "/v1/consensus",
            "/v1/outcomes",
            "/v1/quality",
            "/v1/lineage",
            "/v1/accuracy/history",
            "/v1/accuracy/rankings",
            "/v1/accuracy/contrarian",
            "/v1/graphs",
        }:
            self._send(HTTPStatus.OK, {"ok": True}, include_body=False)
        else:
            self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"}, include_body=False)

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        params = parse_qs(parsed.query)
        try:
            with open_read_only(self.server.database_path) as conn:
                service = LocalQueryService(conn)
                if parsed.path == "/healthz":
                    payload = {"ok": True, "privacy": "local_private_read_only", "capabilities": service.capabilities()}
                elif parsed.path == "/v1/capabilities":
                    payload = service.capabilities()
                elif parsed.path == "/v1/search":
                    payload = service.search(_one(params, "q"), limit=_limit(_one(params, "limit")))
                elif parsed.path == "/v1/subjects":
                    payload = service.subjects(query=_one(params, "q") or None, limit=_limit(_one(params, "limit")))
                elif parsed.path == "/v1/people":
                    payload = service.people(query=_one(params, "q") or None, limit=_limit(_one(params, "limit")))
                elif parsed.path == "/v1/networks":
                    payload = service.networks(
                        person_id=_one(params, "person_id") or None,
                        source_id=_one(params, "source_id") or None,
                        claim_id=_one(params, "claim_id") or None,
                        limit=_limit(_one(params, "limit")),
                    )
                elif parsed.path == "/v1/consensus":
                    payload = service.consensus(claim_id=_one(params, "claim_id") or None, limit=_limit(_one(params, "limit")))
                elif parsed.path == "/v1/accuracy/history":
                    payload = service.accuracy_history(
                        _one(params, "person_id"),
                        as_of=_one(params, "as_of") or None,
                        observed_from=_one(params, "observed_from") or None,
                        observed_through=_one(params, "observed_through") or None,
                        minimum_resolved_claims=int(_one(params, "minimum_resolved_claims") or 10),
                    )
                elif parsed.path == "/v1/accuracy/rankings":
                    payload = service.accuracy_rankings(
                        as_of=_one(params, "as_of") or None,
                        observed_from=_one(params, "observed_from") or None,
                        observed_through=_one(params, "observed_through") or None,
                        minimum_resolved_claims=int(_one(params, "minimum_resolved_claims") or 10),
                        limit=_limit(_one(params, "limit")),
                    )
                elif parsed.path == "/v1/accuracy/contrarian":
                    payload = service.contrarian_success(
                        _one(params, "person_id"),
                        as_of=_one(params, "as_of") or None,
                        observed_from=_one(params, "observed_from") or None,
                        observed_through=_one(params, "observed_through") or None,
                    )
                elif parsed.path == "/v1/graphs":
                    payload = service.graphs(
                        as_of=_one(params, "as_of") or None,
                        limit=_limit(_one(params, "limit")),
                    )
                elif parsed.path == "/v1/outcomes":
                    payload = service.outcomes(
                        claim_id=_one(params, "claim_id") or None,
                        person_id=_one(params, "person_id") or None,
                        limit=_limit(_one(params, "limit")),
                    )
                elif parsed.path == "/v1/quality":
                    payload = service.quality(limit=_limit(_one(params, "limit")))
                elif parsed.path == "/v1/lineage":
                    identifier = _one(params, "id")
                    if not identifier:
                        raise ValueError("id is required")
                    payload = service.lineage(identifier, kind=_one(params, "kind") or "claim", limit=_limit(_one(params, "limit"), 100))
                else:
                    self._send(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})
                    return
            self._send(HTTPStatus.OK, payload)
        except ValueError as exc:
            self._send(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
        except sqlite3.Error as exc:
            self._send(HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": f"query_unavailable: {exc}"})

    def do_POST(self) -> None:
        self._send(HTTPStatus.METHOD_NOT_ALLOWED, {"ok": False, "error": "read_only"})

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send(self, status: HTTPStatus, payload: dict[str, Any], *, include_body: bool = True) -> None:
        body = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)


def _one(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key) or []
    return values[0] if values else ""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local/private read-only PIF query API")
    parser.add_argument("--db", default=str(db_path()))
    parser.add_argument("--host", default=os.environ.get("PIF_QUERY_HOST", DEFAULT_HOST))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PIF_QUERY_PORT", str(DEFAULT_PORT))))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        raise RuntimeError("The private query API must not run on Railway")
    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        raise RuntimeError("The private query API may bind only to loopback")
    database = Path(args.db).expanduser().resolve()
    if not database.is_file():
        raise FileNotFoundError(database)
    server = QueryHTTPServer((args.host, args.port), database)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
