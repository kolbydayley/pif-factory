"""Accepted-only graph and disagreement projections.

This module is deliberately a projection layer, not a semantic layer.  Every
identity, role, affiliation, claim relation, and contrarian classification it
uses must already be present in a ``current_accepted_*`` surface.  It never
falls back to legacy tables and it does not infer equivalence, identity, or
claim meaning.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Callable, Iterable


PROJECTION_SCHEMA_VERSION = "accepted_graph_projection_v1"

_APPEARANCE_SURFACES = frozenset(
    {
        "current_accepted_person_appearances",
    }
)
_AFFILIATION_SURFACES = frozenset(
    {
        "current_accepted_person_appearances",
        "current_accepted_source_affiliations",
    }
)
_RELATION_SURFACES = frozenset(
    {
        "current_accepted_atomic_claims",
        "current_accepted_position_observations",
        "current_accepted_claim_relations",
    }
)
_CONTRARIAN_SURFACES = frozenset(
    {
        "current_accepted_atomic_claims",
        "current_accepted_position_observations",
        "current_accepted_contrarian_snapshots",
    }
)


def _normalize_as_of(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("as_of is required")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError as exc:
        raise ValueError("as_of must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("as_of must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _accepted_instant(value: Any) -> datetime:
    text = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError as exc:
        raise ValueError("accepted timestamp is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("accepted timestamp lacks a timezone")
    return parsed.astimezone(timezone.utc)


def _limit(value: int) -> int:
    size = int(value)
    if size < 1:
        raise ValueError("limit must be positive")
    return min(size, 1_000)


def _relations(conn: sqlite3.Connection) -> set[str]:
    names = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_schema WHERE type IN ('table', 'view')"
        ).fetchall()
    }
    names.update(
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_temp_master WHERE type IN ('table', 'view')"
        ).fetchall()
    )
    return names


def _rows(conn: sqlite3.Connection, sql: str, parameters: Iterable[Any]) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, tuple(parameters))
    columns = [str(column[0]) for column in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _unavailable(
    projection: str,
    as_of: str,
    *,
    missing_surfaces: Iterable[str] = (),
    reason: str = "required accepted surfaces are unavailable",
) -> dict[str, Any]:
    return {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "projection": projection,
        "authority": "current_accepted_only",
        "available": False,
        "as_of": as_of,
        "row_count": 0,
        "rows": [],
        "missing_surfaces": sorted(set(missing_surfaces)),
        "unavailable_reason": reason,
    }


def _available(projection: str, as_of: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "projection": projection,
        "authority": "current_accepted_only",
        "available": True,
        "as_of": as_of,
        "row_count": len(rows),
        "rows": rows,
        "missing_surfaces": [],
        "unavailable_reason": None,
    }


def _project(
    conn: sqlite3.Connection,
    *,
    projection: str,
    as_of: str,
    required_surfaces: frozenset[str],
    build: Callable[[str], list[dict[str, Any]]],
) -> dict[str, Any]:
    normalized_as_of = _normalize_as_of(as_of)
    try:
        missing = required_surfaces - _relations(conn)
    except sqlite3.Error:
        return _unavailable(
            projection,
            normalized_as_of,
            missing_surfaces=required_surfaces,
            reason="accepted surface inventory could not be read",
        )
    if missing:
        return _unavailable(projection, normalized_as_of, missing_surfaces=missing)
    try:
        return _available(projection, normalized_as_of, build(normalized_as_of))
    except (json.JSONDecodeError, sqlite3.Error, TypeError, ValueError):
        # An accepted view with a malformed contract is not permission to use
        # legacy data or emit a partial graph.  Suppress the whole projection.
        return _unavailable(
            projection,
            normalized_as_of,
            reason="accepted surfaces did not satisfy the projection contract",
        )


def shared_guest_graph(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Project source-to-source edges created by accepted guest appearances."""

    size = _limit(limit)

    def build(cutoff: str) -> list[dict[str, Any]]:
        evidence = _rows(
            conn,
            """
            SELECT
              left_appearance.corpus_release_id,
              left_appearance.source_id AS source_a_id,
              right_appearance.source_id AS source_b_id,
              left_appearance.canonical_person_id,
              left_appearance.id AS appearance_a_id,
              right_appearance.id AS appearance_b_id,
              left_appearance.episode_id AS episode_a_id,
              right_appearance.episode_id AS episode_b_id
            FROM current_accepted_person_appearances AS left_appearance
            JOIN current_accepted_person_appearances AS right_appearance
              ON right_appearance.corpus_release_id = left_appearance.corpus_release_id
             AND right_appearance.canonical_person_id = left_appearance.canonical_person_id
             AND right_appearance.source_id > left_appearance.source_id
            WHERE left_appearance.role = 'guest'
              AND right_appearance.role = 'guest'
              AND julianday(left_appearance.appeared_at) <= julianday(?)
              AND julianday(right_appearance.appeared_at) <= julianday(?)
            ORDER BY left_appearance.corpus_release_id,
                     left_appearance.source_id,
                     right_appearance.source_id,
                     left_appearance.canonical_person_id,
                     left_appearance.id,
                     right_appearance.id
            """,
            (cutoff, cutoff),
        )
        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in evidence:
            key = (row["corpus_release_id"], row["source_a_id"], row["source_b_id"])
            edge = grouped.setdefault(
                key,
                {
                    "corpus_release_id": row["corpus_release_id"],
                    "as_of": cutoff,
                    "source_a_id": row["source_a_id"],
                    "source_b_id": row["source_b_id"],
                    "person_ids": set(),
                    "evidence_appearance_ids": set(),
                    "evidence_episode_ids": set(),
                },
            )
            edge["person_ids"].add(row["canonical_person_id"])
            edge["evidence_appearance_ids"].update(
                (row["appearance_a_id"], row["appearance_b_id"])
            )
            edge["evidence_episode_ids"].update((row["episode_a_id"], row["episode_b_id"]))
        rows: list[dict[str, Any]] = []
        for edge in grouped.values():
            edge["person_ids"] = sorted(edge["person_ids"])
            edge["evidence_appearance_ids"] = sorted(edge["evidence_appearance_ids"])
            edge["evidence_episode_ids"] = sorted(edge["evidence_episode_ids"])
            edge["shared_guest_count"] = len(edge["person_ids"])
            rows.append(edge)
        rows.sort(
            key=lambda edge: (
                -edge["shared_guest_count"],
                edge["source_a_id"],
                edge["source_b_id"],
            )
        )
        return rows[:size]

    return _project(
        conn,
        projection="shared_guest_graph",
        as_of=as_of,
        required_surfaces=_APPEARANCE_SURFACES,
        build=build,
    )


def coappearance_graph(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Project person-to-person edges from accepted same-episode appearances."""

    size = _limit(limit)

    def build(cutoff: str) -> list[dict[str, Any]]:
        evidence = _rows(
            conn,
            """
            SELECT
              left_appearance.corpus_release_id,
              left_appearance.canonical_person_id AS person_a_id,
              right_appearance.canonical_person_id AS person_b_id,
              left_appearance.id AS appearance_a_id,
              right_appearance.id AS appearance_b_id,
              left_appearance.role AS person_a_role,
              right_appearance.role AS person_b_role,
              left_appearance.source_id,
              left_appearance.episode_id
            FROM current_accepted_person_appearances AS left_appearance
            JOIN current_accepted_person_appearances AS right_appearance
              ON right_appearance.corpus_release_id = left_appearance.corpus_release_id
             AND right_appearance.episode_id = left_appearance.episode_id
             AND right_appearance.source_id = left_appearance.source_id
             AND right_appearance.canonical_person_id > left_appearance.canonical_person_id
            WHERE julianday(left_appearance.appeared_at) <= julianday(?)
              AND julianday(right_appearance.appeared_at) <= julianday(?)
            ORDER BY left_appearance.corpus_release_id,
                     left_appearance.canonical_person_id,
                     right_appearance.canonical_person_id,
                     left_appearance.episode_id,
                     left_appearance.id,
                     right_appearance.id
            """,
            (cutoff, cutoff),
        )
        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in evidence:
            key = (row["corpus_release_id"], row["person_a_id"], row["person_b_id"])
            edge = grouped.setdefault(
                key,
                {
                    "corpus_release_id": row["corpus_release_id"],
                    "as_of": cutoff,
                    "person_a_id": row["person_a_id"],
                    "person_b_id": row["person_b_id"],
                    "source_ids": set(),
                    "evidence_episode_ids": set(),
                    "evidence_appearance_ids": set(),
                    "appearance_roles": set(),
                },
            )
            edge["source_ids"].add(row["source_id"])
            edge["evidence_episode_ids"].add(row["episode_id"])
            edge["evidence_appearance_ids"].update(
                (row["appearance_a_id"], row["appearance_b_id"])
            )
            edge["appearance_roles"].add(
                f'{row["person_a_role"]}:{row["person_b_role"]}'
            )
        rows: list[dict[str, Any]] = []
        for edge in grouped.values():
            edge["source_ids"] = sorted(edge["source_ids"])
            edge["evidence_episode_ids"] = sorted(edge["evidence_episode_ids"])
            edge["evidence_appearance_ids"] = sorted(edge["evidence_appearance_ids"])
            edge["appearance_roles"] = sorted(edge["appearance_roles"])
            edge["coappearance_count"] = len(edge["evidence_episode_ids"])
            rows.append(edge)
        rows.sort(
            key=lambda edge: (
                -edge["coappearance_count"],
                edge["person_a_id"],
                edge["person_b_id"],
            )
        )
        return rows[:size]

    return _project(
        conn,
        projection="coappearance_graph",
        as_of=as_of,
        required_surfaces=_APPEARANCE_SURFACES,
        build=build,
    )


def cross_network_speaker_graph(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Project affiliation-to-affiliation edges traversed by accepted speakers.

    Endpoint keys include the reviewed affiliation kind so a publisher, owner,
    network, and independent designation cannot be conflated merely because
    their text keys match.
    """

    size = _limit(limit)

    def build(cutoff: str) -> list[dict[str, Any]]:
        evidence = _rows(
            conn,
            """
            WITH eligible AS (
              SELECT
                appearances.corpus_release_id,
                appearances.id AS appearance_id,
                appearances.canonical_person_id,
                appearances.source_id,
                appearances.episode_id,
                appearances.appeared_at,
                affiliations.id AS affiliation_id,
                affiliations.affiliation_kind,
                affiliations.affiliation_key,
                affiliations.affiliation_name,
                affiliations.affiliation_kind || ':' || affiliations.affiliation_key
                  AS endpoint_key
              FROM current_accepted_person_appearances AS appearances
              JOIN current_accepted_source_affiliations AS affiliations
                ON affiliations.corpus_release_id = appearances.corpus_release_id
               AND affiliations.source_id = appearances.source_id
               AND (
                 affiliations.valid_from IS NULL
                 OR julianday(affiliations.valid_from) <= julianday(appearances.appeared_at)
               )
               AND (
                 affiliations.valid_to IS NULL
                 OR julianday(affiliations.valid_to) >= julianday(appearances.appeared_at)
               )
              WHERE julianday(appearances.appeared_at) <= julianday(?)
            )
            SELECT
              left_endpoint.corpus_release_id,
              left_endpoint.endpoint_key AS endpoint_a_key,
              left_endpoint.affiliation_kind AS endpoint_a_kind,
              left_endpoint.affiliation_key AS endpoint_a_affiliation_key,
              left_endpoint.affiliation_name AS endpoint_a_name,
              right_endpoint.endpoint_key AS endpoint_b_key,
              right_endpoint.affiliation_kind AS endpoint_b_kind,
              right_endpoint.affiliation_key AS endpoint_b_affiliation_key,
              right_endpoint.affiliation_name AS endpoint_b_name,
              left_endpoint.canonical_person_id,
              left_endpoint.source_id AS source_a_id,
              right_endpoint.source_id AS source_b_id,
              left_endpoint.episode_id AS episode_a_id,
              right_endpoint.episode_id AS episode_b_id,
              left_endpoint.appearance_id AS appearance_a_id,
              right_endpoint.appearance_id AS appearance_b_id,
              left_endpoint.affiliation_id AS affiliation_a_id,
              right_endpoint.affiliation_id AS affiliation_b_id
            FROM eligible AS left_endpoint
            JOIN eligible AS right_endpoint
              ON right_endpoint.corpus_release_id = left_endpoint.corpus_release_id
             AND right_endpoint.canonical_person_id = left_endpoint.canonical_person_id
             AND right_endpoint.source_id <> left_endpoint.source_id
             AND right_endpoint.endpoint_key > left_endpoint.endpoint_key
            ORDER BY left_endpoint.corpus_release_id,
                     left_endpoint.endpoint_key,
                     right_endpoint.endpoint_key,
                     left_endpoint.canonical_person_id,
                     left_endpoint.appearance_id,
                     right_endpoint.appearance_id
            """,
            (cutoff,),
        )
        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in evidence:
            key = (
                row["corpus_release_id"],
                row["endpoint_a_key"],
                row["endpoint_b_key"],
            )
            edge = grouped.setdefault(
                key,
                {
                    "corpus_release_id": row["corpus_release_id"],
                    "as_of": cutoff,
                    "endpoint_a": {
                        "key": row["endpoint_a_key"],
                        "kind": row["endpoint_a_kind"],
                        "affiliation_key": row["endpoint_a_affiliation_key"],
                        "name": row["endpoint_a_name"],
                    },
                    "endpoint_b": {
                        "key": row["endpoint_b_key"],
                        "kind": row["endpoint_b_kind"],
                        "affiliation_key": row["endpoint_b_affiliation_key"],
                        "name": row["endpoint_b_name"],
                    },
                    "person_ids": set(),
                    "source_ids": set(),
                    "evidence_episode_ids": set(),
                    "evidence_appearance_ids": set(),
                    "evidence_affiliation_ids": set(),
                },
            )
            edge["person_ids"].add(row["canonical_person_id"])
            edge["source_ids"].update((row["source_a_id"], row["source_b_id"]))
            edge["evidence_episode_ids"].update((row["episode_a_id"], row["episode_b_id"]))
            edge["evidence_appearance_ids"].update(
                (row["appearance_a_id"], row["appearance_b_id"])
            )
            edge["evidence_affiliation_ids"].update(
                (row["affiliation_a_id"], row["affiliation_b_id"])
            )
        rows: list[dict[str, Any]] = []
        for edge in grouped.values():
            for field in (
                "person_ids",
                "source_ids",
                "evidence_episode_ids",
                "evidence_appearance_ids",
                "evidence_affiliation_ids",
            ):
                edge[field] = sorted(edge[field])
            edge["speaker_count"] = len(edge["person_ids"])
            rows.append(edge)
        rows.sort(
            key=lambda edge: (
                -edge["speaker_count"],
                edge["endpoint_a"]["key"],
                edge["endpoint_b"]["key"],
            )
        )
        return rows[:size]

    return _project(
        conn,
        projection="cross_network_speaker_graph",
        as_of=as_of,
        required_surfaces=_AFFILIATION_SURFACES,
        build=build,
    )


def _contradiction_rows(conn: sqlite3.Connection, cutoff: str) -> list[dict[str, Any]]:
    return _rows(
        conn,
        """
        SELECT
          relations.id AS relation_id,
          relations.corpus_release_id,
          relations.pipeline_run_id,
          relations.source_claim_id,
          relations.target_claim_id,
          relations.temporal_scope,
          relations.confidence,
          relations.rationale,
          relations.judge_model,
          relations.decided_at,
          source_position.id AS source_position_id,
          source_position.canonical_person_id AS source_person_id,
          source_position.subject_id AS source_subject_id,
          source_position.variant_id AS source_variant_id,
          source_position.position AS source_position,
          source_position.observed_at AS source_observed_at,
          target_position.id AS target_position_id,
          target_position.canonical_person_id AS target_person_id,
          target_position.subject_id AS target_subject_id,
          target_position.variant_id AS target_variant_id,
          target_position.position AS target_position,
          target_position.observed_at AS target_observed_at,
          source_claim.source_id AS source_source_id,
          source_claim.episode_id AS source_episode_id,
          target_claim.source_id AS target_source_id,
          target_claim.episode_id AS target_episode_id
        FROM current_accepted_claim_relations AS relations
        JOIN current_accepted_position_observations AS source_position
          ON source_position.atomic_claim_id = relations.source_claim_id
         AND source_position.corpus_release_id = relations.corpus_release_id
        JOIN current_accepted_position_observations AS target_position
          ON target_position.atomic_claim_id = relations.target_claim_id
         AND target_position.corpus_release_id = relations.corpus_release_id
        JOIN current_accepted_atomic_claims AS source_claim
          ON source_claim.id = relations.source_claim_id
         AND source_claim.corpus_release_id = relations.corpus_release_id
        JOIN current_accepted_atomic_claims AS target_claim
          ON target_claim.id = relations.target_claim_id
         AND target_claim.corpus_release_id = relations.corpus_release_id
        WHERE relations.relation = 'contradicts'
          AND julianday(relations.decided_at) <= julianday(?)
          AND julianday(source_position.observed_at) <= julianday(?)
          AND julianday(target_position.observed_at) <= julianday(?)
        ORDER BY relations.decided_at DESC, relations.id,
                 source_position.id, target_position.id
        """,
        (cutoff, cutoff, cutoff),
    )


def cross_speaker_disagreements(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Expose accepted contradiction judgments involving different people."""

    size = _limit(limit)

    def build(cutoff: str) -> list[dict[str, Any]]:
        rows = []
        for row in _contradiction_rows(conn, cutoff):
            if row["source_person_id"] == row["target_person_id"]:
                continue
            row["record_type"] = "cross_speaker_disagreement"
            row["as_of"] = cutoff
            row["evidence_ids"] = {
                "relation_id": row["relation_id"],
                "claim_ids": [row["source_claim_id"], row["target_claim_id"]],
                "position_ids": [row["source_position_id"], row["target_position_id"]],
                "episode_ids": sorted(
                    {row["source_episode_id"], row["target_episode_id"]}
                ),
            }
            rows.append(row)
        return rows[:size]

    return _project(
        conn,
        projection="cross_speaker_disagreements",
        as_of=as_of,
        required_surfaces=_RELATION_SURFACES,
        build=build,
    )


def same_speaker_position_changes(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Expose accepted contradictions between observations by the same person.

    The only classification performed here is temporal: unequal observation
    times are presented as a position change, while simultaneous observations
    are presented as a self-contradiction.  The underlying semantic judgment is
    always the accepted LLM ``contradicts`` relation.
    """

    size = _limit(limit)

    def build(cutoff: str) -> list[dict[str, Any]]:
        rows = []
        for row in _contradiction_rows(conn, cutoff):
            if row["source_person_id"] != row["target_person_id"]:
                continue
            source_instant = _accepted_instant(row["source_observed_at"])
            target_instant = _accepted_instant(row["target_observed_at"])
            source_first = (
                source_instant,
                row["source_claim_id"],
            ) <= (
                target_instant,
                row["target_claim_id"],
            )
            earlier_prefix, later_prefix = (
                ("source", "target") if source_first else ("target", "source")
            )
            row["record_type"] = (
                "self_contradiction"
                if source_instant == target_instant
                else "position_change"
            )
            row["canonical_person_id"] = row["source_person_id"]
            row["earlier_claim_id"] = row[f"{earlier_prefix}_claim_id"]
            row["later_claim_id"] = row[f"{later_prefix}_claim_id"]
            row["earlier_observed_at"] = row[f"{earlier_prefix}_observed_at"]
            row["later_observed_at"] = row[f"{later_prefix}_observed_at"]
            row["as_of"] = cutoff
            row["evidence_ids"] = {
                "relation_id": row["relation_id"],
                "claim_ids": [row["source_claim_id"], row["target_claim_id"]],
                "position_ids": [row["source_position_id"], row["target_position_id"]],
                "episode_ids": sorted(
                    {row["source_episode_id"], row["target_episode_id"]}
                ),
            }
            rows.append(row)
        return rows[:size]

    return _project(
        conn,
        projection="same_speaker_position_changes",
        as_of=as_of,
        required_surfaces=_RELATION_SURFACES,
        build=build,
    )


def corpus_contrarian_records(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Expose accepted corpus-consensus classifications without truth claims."""

    size = _limit(limit)

    def build(cutoff: str) -> list[dict[str, Any]]:
        rows = _rows(
            conn,
            """
            SELECT
              snapshots.id AS contrarian_snapshot_id,
              snapshots.corpus_release_id,
              snapshots.pipeline_run_id,
              snapshots.target_claim_id,
              snapshots.consensus_snapshot_id,
              snapshots.calculation_version,
              snapshots.as_of AS snapshot_as_of,
              snapshots.window_start,
              snapshots.window_end,
              snapshots.window_days,
              snapshots.people_count,
              snapshots.show_count,
              snapshots.network_count,
              snapshots.dominant_bucket,
              snapshots.dominant_share,
              snapshots.target_share,
              snapshots.classification,
              snapshots.is_contrarian,
              snapshots.exclusion_reason,
              snapshots.inputs_sha256,
              snapshots.included_claim_ids_json,
              positions.id AS position_id,
              positions.canonical_person_id,
              positions.subject_id,
              positions.variant_id,
              positions.position,
              positions.observed_at,
              claims.source_id,
              claims.episode_id
            FROM current_accepted_contrarian_snapshots AS snapshots
            JOIN current_accepted_atomic_claims AS claims
              ON claims.id = snapshots.target_claim_id
             AND claims.corpus_release_id = snapshots.corpus_release_id
            JOIN current_accepted_position_observations AS positions
              ON positions.atomic_claim_id = snapshots.target_claim_id
             AND positions.corpus_release_id = snapshots.corpus_release_id
            WHERE julianday(snapshots.as_of) <= julianday(?)
              AND julianday(positions.observed_at) <= julianday(?)
            ORDER BY snapshots.as_of DESC, snapshots.id, positions.id
            LIMIT ?
            """,
            (cutoff, cutoff, size),
        )
        for row in rows:
            included = json.loads(row.pop("included_claim_ids_json"))
            if not isinstance(included, list) or not all(
                isinstance(item, str) for item in included
            ):
                raise ValueError("included_claim_ids_json must be a list of IDs")
            row["is_contrarian"] = bool(row["is_contrarian"])
            row["as_of"] = cutoff
            row["consensus_is_not_truth"] = True
            row["evidence_ids"] = {
                "contrarian_snapshot_id": row["contrarian_snapshot_id"],
                "consensus_snapshot_id": row["consensus_snapshot_id"],
                "target_claim_id": row["target_claim_id"],
                "position_id": row["position_id"],
                "preceding_consensus_claim_ids": included,
            }
        return rows

    return _project(
        conn,
        projection="corpus_contrarian_records",
        as_of=as_of,
        required_surfaces=_CONTRARIAN_SURFACES,
        build=build,
    )


def accepted_graph_bundle(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Return every accepted-only graph and distinction as one read model."""

    normalized = _normalize_as_of(as_of)
    projections = {
        "shared_guest_graph": shared_guest_graph(conn, as_of=normalized, limit=limit),
        "coappearance_graph": coappearance_graph(conn, as_of=normalized, limit=limit),
        "cross_network_speaker_graph": cross_network_speaker_graph(
            conn, as_of=normalized, limit=limit
        ),
        "cross_speaker_disagreements": cross_speaker_disagreements(
            conn, as_of=normalized, limit=limit
        ),
        "same_speaker_position_changes": same_speaker_position_changes(
            conn, as_of=normalized, limit=limit
        ),
        "corpus_contrarian_records": corpus_contrarian_records(
            conn, as_of=normalized, limit=limit
        ),
    }
    return {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "authority": "current_accepted_only",
        "as_of": normalized,
        "available": all(item["available"] for item in projections.values()),
        "projections": projections,
    }


__all__ = [
    "PROJECTION_SCHEMA_VERSION",
    "accepted_graph_bundle",
    "coappearance_graph",
    "corpus_contrarian_records",
    "cross_network_speaker_graph",
    "cross_speaker_disagreements",
    "same_speaker_position_changes",
    "shared_guest_graph",
]
