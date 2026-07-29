"""Accepted-only accuracy histories and rankings.

The functions in this module are deterministic projections over the canonical
``current_accepted_*`` surfaces.  They never infer a speaker, claim meaning,
relation, contrarian classification, or outcome.  If any required accepted
surface is missing or malformed, scoring fails closed instead of consulting a
legacy table.

``as_of`` is a knowledge cutoff.  A resolution or contrarian judgment is
visible only when both its declared ``as_of`` and its immutable ``created_at``
are no later than the cutoff.  Claim-period filters use ``observed_at``.
Categorical accuracy and Brier loss are always reported separately.
"""

from __future__ import annotations

import datetime as dt
import math
import sqlite3
from typing import Any, Iterable, Mapping

from .util import UTC, now_iso, parse_datetime


DEFAULT_MINIMUM_RESOLVED_CLAIMS = 10

_CATEGORICAL_SCORES: Mapping[str, float | None] = {
    "true": 1.0,
    "mixed": 0.5,
    "false": 0.0,
    "unresolved": None,
    "unverifiable": None,
}

_REQUIRED_SURFACES: Mapping[str, frozenset[str]] = {
    "current_accepted_corpus_releases": frozenset(
        {"id", "release_version", "manifest_sha256", "promoted_at"}
    ),
    "current_accepted_people": frozenset({"id", "display_name"}),
    "current_accepted_atomic_claims": frozenset(
        {
            "id",
            "corpus_release_id",
            "forecast_probability",
            "observed_at",
            "created_at",
        }
    ),
    "current_accepted_position_observations": frozenset(
        {
            "id",
            "corpus_release_id",
            "atomic_claim_id",
            "canonical_person_id",
        }
    ),
    "current_accepted_outcome_resolutions": frozenset(
        {
            "id",
            "claim_id",
            "corpus_release_id",
            "outcome",
            "categorical_score",
            "brier_score",
            "resolved_at",
            "as_of",
            "created_at",
        }
    ),
    "current_accepted_contrarian_snapshots": frozenset(
        {
            "id",
            "target_claim_id",
            "corpus_release_id",
            "classification",
            "as_of",
            "created_at",
        }
    ),
}


class AccuracyUnavailableError(RuntimeError):
    """The accepted production projection is not available for scoring."""


class AccuracyDataError(RuntimeError):
    """An accepted row violates the deterministic scoring contract."""


def person_accuracy_history(
    conn: sqlite3.Connection,
    person_id: str,
    *,
    as_of: str | None = None,
    observed_from: str | None = None,
    observed_through: str | None = None,
    minimum_resolved_claims: int = DEFAULT_MINIMUM_RESOLVED_CLAIMS,
) -> dict[str, Any]:
    """Return one accepted person's auditable accuracy history.

    ``history`` contains one current accepted outcome per claim, ordered by the
    outcome's evidence date.  Running categorical and Brier metrics make every
    point reproducible.  Unresolved and unverifiable outcomes remain in the
    history and coverage counts but never enter a score denominator.
    """

    context = _accepted_context(conn)
    cutoff = _cutoff(as_of)
    period_start = _optional_time(observed_from, "observed_from")
    period_end = _optional_time(observed_through, "observed_through")
    if period_start and period_end and period_end < period_start:
        raise ValueError("observed_through cannot precede observed_from")
    minimum = _positive_integer(minimum_resolved_claims, "minimum_resolved_claims")

    people = _fetch_dicts(
        conn,
        "SELECT id, display_name FROM current_accepted_people WHERE id = ?",
        (str(person_id),),
    )
    if len(people) != 1:
        raise AccuracyUnavailableError(
            f"person {person_id!r} is not a current accepted canonical person"
        )
    person = people[0]

    claim_rows = _fetch_dicts(
        conn,
        """
        SELECT claims.id, claims.corpus_release_id,
               positions.canonical_person_id, positions.id AS position_id,
               claims.forecast_probability, claims.observed_at, claims.created_at
        FROM current_accepted_atomic_claims AS claims
        JOIN current_accepted_position_observations AS positions
          ON positions.atomic_claim_id = claims.id
         AND positions.corpus_release_id = claims.corpus_release_id
        WHERE claims.corpus_release_id = ? AND positions.canonical_person_id = ?
        ORDER BY claims.observed_at, claims.id, positions.id
        """,
        (context["release_id"], str(person_id)),
    )
    claims: dict[str, dict[str, Any]] = {}
    for claim in claim_rows:
        claim_id = str(claim["id"])
        if claim_id in claims:
            raise AccuracyDataError(
                "current_accepted_position_observations exposed more than one "
                f"accepted person position for claim {claim_id}"
            )
        observed = _row_time(claim, "observed_at", "claim")
        created = _row_time(claim, "created_at", "claim")
        if observed > cutoff or created > cutoff:
            continue
        if period_start and observed < period_start:
            continue
        if period_end and observed > period_end:
            continue
        probability = claim.get("forecast_probability")
        if probability is not None:
            claim["forecast_probability"] = _probability(
                probability, f"claim {claim['id']} forecast_probability"
            )
        claims[claim_id] = claim

    outcome_rows = _fetch_dicts(
        conn,
        """
        SELECT id, claim_id, corpus_release_id, outcome, categorical_score,
               brier_score, resolved_at, as_of, created_at
        FROM current_accepted_outcome_resolutions
        WHERE corpus_release_id = ?
        ORDER BY as_of, created_at, id
        """,
        (context["release_id"],),
    )
    outcomes_by_claim: dict[str, dict[str, Any]] = {}
    for outcome in outcome_rows:
        claim_id = str(outcome["claim_id"])
        if claim_id not in claims:
            continue
        evidence_at = _row_time(outcome, "as_of", "outcome resolution")
        created_at = _row_time(outcome, "created_at", "outcome resolution")
        if evidence_at > cutoff or created_at > cutoff:
            continue
        if claim_id in outcomes_by_claim:
            raise AccuracyDataError(
                "current_accepted_outcome_resolutions exposed more than one "
                f"current row for claim {claim_id}"
            )
        outcome["_evidence_at"] = evidence_at
        outcome["_created_at"] = created_at
        outcomes_by_claim[claim_id] = outcome

    history: list[dict[str, Any]] = []
    categorical_numerator = 0.0
    categorical_denominator = 0
    brier_numerator = 0.0
    brier_denominator = 0
    unresolved_count = 0
    unverifiable_count = 0
    ordered_outcomes = sorted(
        outcomes_by_claim.values(),
        key=lambda row: (row["_evidence_at"], row["_created_at"], str(row["id"])),
    )
    for outcome in ordered_outcomes:
        claim = claims[str(outcome["claim_id"])]
        categorical, brier = _validated_scores(outcome, claim)
        if categorical is not None:
            categorical_numerator += categorical
            categorical_denominator += 1
        elif outcome["outcome"] == "unresolved":
            unresolved_count += 1
        elif outcome["outcome"] == "unverifiable":
            unverifiable_count += 1
        if brier is not None:
            brier_numerator += brier
            brier_denominator += 1
        history.append(
            {
                "claim_id": str(outcome["claim_id"]),
                "resolution_id": str(outcome["id"]),
                "claim_observed_at": _iso(_row_time(claim, "observed_at", "claim")),
                "outcome": str(outcome["outcome"]),
                "outcome_as_of": _iso(outcome["_evidence_at"]),
                "resolved_at": _iso(
                    _row_time(outcome, "resolved_at", "outcome resolution")
                ),
                "categorical_score": categorical,
                "brier_score": brier,
                "running_categorical_numerator": categorical_numerator,
                "running_categorical_denominator": categorical_denominator,
                "running_categorical_accuracy": _mean(
                    categorical_numerator, categorical_denominator
                ),
                "running_brier_numerator": brier_numerator,
                "running_brier_denominator": brier_denominator,
                "running_brier_mean": _mean(brier_numerator, brier_denominator),
                "corpus_release_id": context["release_id"],
            }
        )

    accepted_claim_count = len(claims)
    accepted_resolution_count = len(ordered_outcomes)
    no_resolution_count = accepted_claim_count - accepted_resolution_count
    excluded_count = unresolved_count + unverifiable_count
    contrarian = _contrarian_success(
        conn,
        context=context,
        claims=claims,
        outcomes=outcomes_by_claim,
        cutoff=cutoff,
    )
    categorical_accuracy = _mean(categorical_numerator, categorical_denominator)
    return {
        "person_id": str(person["id"]),
        "display_name": str(person["display_name"]),
        "release": context,
        "as_of": _iso(cutoff),
        "observed_from": _iso(period_start) if period_start else None,
        "observed_through": _iso(period_end) if period_end else None,
        "accepted_claim_count": accepted_claim_count,
        "accepted_resolution_count": accepted_resolution_count,
        "no_resolution_count": no_resolution_count,
        "categorical": {
            "numerator": categorical_numerator,
            "denominator": categorical_denominator,
            "accuracy": categorical_accuracy,
        },
        "brier": {
            "numerator": brier_numerator,
            "denominator": brier_denominator,
            "mean": _mean(brier_numerator, brier_denominator),
        },
        "coverage": {
            "accepted_claim_denominator": accepted_claim_count,
            "resolution_numerator": accepted_resolution_count,
            "resolution_rate": _mean(accepted_resolution_count, accepted_claim_count),
            "resolved_categorical_numerator": categorical_denominator,
            "resolved_categorical_rate": _mean(
                categorical_denominator, accepted_claim_count
            ),
            "unresolved_count": unresolved_count,
            "unverifiable_count": unverifiable_count,
            "score_excluded_count": excluded_count,
            "unresolved_coverage_numerator": excluded_count,
            "unresolved_coverage_denominator": accepted_resolution_count,
            "unresolved_coverage_rate": _mean(
                excluded_count, accepted_resolution_count
            ),
        },
        "ranking_eligibility": {
            "minimum_resolved_claims": minimum,
            "resolved_categorical_claims": categorical_denominator,
            "eligible": categorical_denominator >= minimum,
        },
        "contrarian_success": contrarian,
        "history": history,
    }


def person_accuracy_rankings(
    conn: sqlite3.Connection,
    *,
    as_of: str | None = None,
    observed_from: str | None = None,
    observed_through: str | None = None,
    minimum_resolved_claims: int = DEFAULT_MINIMUM_RESOLVED_CLAIMS,
) -> dict[str, Any]:
    """Rank only people meeting the resolved categorical-claim threshold."""

    context = _accepted_context(conn)
    cutoff = _cutoff(as_of)
    minimum = _positive_integer(minimum_resolved_claims, "minimum_resolved_claims")
    people = _fetch_dicts(
        conn,
        "SELECT id, display_name FROM current_accepted_people ORDER BY display_name, id",
    )
    summaries = [
        person_accuracy_history(
            conn,
            str(person["id"]),
            as_of=_iso(cutoff),
            observed_from=observed_from,
            observed_through=observed_through,
            minimum_resolved_claims=minimum,
        )
        for person in people
    ]
    eligible = [
        summary
        for summary in summaries
        if summary["ranking_eligibility"]["eligible"]
    ]
    eligible.sort(
        key=lambda summary: (
            -float(summary["categorical"]["accuracy"]),
            -int(summary["categorical"]["denominator"]),
            str(summary["display_name"]).casefold(),
            str(summary["person_id"]),
        )
    )
    rankings: list[dict[str, Any]] = []
    for rank, summary in enumerate(eligible, start=1):
        contrarian = summary["contrarian_success"]
        rankings.append(
            {
                "rank": rank,
                "person_id": summary["person_id"],
                "display_name": summary["display_name"],
                "categorical_numerator": summary["categorical"]["numerator"],
                "categorical_denominator": summary["categorical"]["denominator"],
                "categorical_accuracy": summary["categorical"]["accuracy"],
                "brier_numerator": summary["brier"]["numerator"],
                "brier_denominator": summary["brier"]["denominator"],
                "brier_mean": summary["brier"]["mean"],
                "accepted_claim_count": summary["accepted_claim_count"],
                "accepted_resolution_count": summary["accepted_resolution_count"],
                "unresolved_count": summary["coverage"]["unresolved_count"],
                "unverifiable_count": summary["coverage"]["unverifiable_count"],
                "unresolved_coverage_numerator": summary["coverage"][
                    "unresolved_coverage_numerator"
                ],
                "unresolved_coverage_denominator": summary["coverage"][
                    "unresolved_coverage_denominator"
                ],
                "contrarian_success_numerator": contrarian["numerator"],
                "contrarian_success_denominator": contrarian["denominator"],
                "contrarian_success_rate": contrarian["rate"],
                "corpus_release_id": context["release_id"],
                "as_of": _iso(cutoff),
                "eligible": True,
            }
        )
    return {
        "release": context,
        "as_of": _iso(cutoff),
        "observed_from": _iso(_optional_time(observed_from, "observed_from"))
        if observed_from
        else None,
        "observed_through": _iso(
            _optional_time(observed_through, "observed_through")
        )
        if observed_through
        else None,
        "minimum_resolved_claims": minimum,
        "accepted_person_count": len(summaries),
        "eligible_person_count": len(rankings),
        "excluded_below_minimum_count": len(summaries) - len(rankings),
        "rankings": rankings,
    }


def person_contrarian_success(
    conn: sqlite3.Connection,
    person_id: str,
    *,
    as_of: str | None = None,
    observed_from: str | None = None,
    observed_through: str | None = None,
) -> dict[str, Any]:
    """Return the accepted contrarian subset of a person's accuracy history."""

    return person_accuracy_history(
        conn,
        person_id,
        as_of=as_of,
        observed_from=observed_from,
        observed_through=observed_through,
    )["contrarian_success"]


def _contrarian_success(
    conn: sqlite3.Connection,
    *,
    context: Mapping[str, Any],
    claims: Mapping[str, Mapping[str, Any]],
    outcomes: Mapping[str, Mapping[str, Any]],
    cutoff: dt.datetime,
) -> dict[str, Any]:
    rows = _fetch_dicts(
        conn,
        """
        SELECT id, target_claim_id, corpus_release_id, classification, as_of,
               created_at
        FROM current_accepted_contrarian_snapshots
        WHERE corpus_release_id = ?
        ORDER BY as_of, created_at, id
        """,
        (context["release_id"],),
    )
    snapshots_by_claim: dict[str, dict[str, Any]] = {}
    for row in rows:
        claim_id = str(row["target_claim_id"])
        if claim_id not in claims:
            continue
        evidence_at = _row_time(row, "as_of", "contrarian snapshot")
        created_at = _row_time(row, "created_at", "contrarian snapshot")
        if evidence_at > cutoff or created_at > cutoff:
            continue
        classification = str(row["classification"])
        if classification not in {
            "contrarian",
            "not_contrarian",
            "insufficient_coverage",
        }:
            raise AccuracyDataError(
                f"contrarian snapshot {row['id']} has invalid classification"
            )
        row["_evidence_at"] = evidence_at
        row["_created_at"] = created_at
        previous = snapshots_by_claim.get(claim_id)
        if previous is None or (
            evidence_at,
            created_at,
            str(row["id"]),
        ) > (
            previous["_evidence_at"],
            previous["_created_at"],
            str(previous["id"]),
        ):
            snapshots_by_claim[claim_id] = row

    observations = {
        claim_id: snapshot
        for claim_id, snapshot in snapshots_by_claim.items()
        if snapshot["classification"] == "contrarian"
    }
    numerator = 0.0
    denominator = 0
    unresolved_count = 0
    unverifiable_count = 0
    cases: list[dict[str, Any]] = []
    for claim_id, snapshot in sorted(
        observations.items(),
        key=lambda item: (
            item[1]["_evidence_at"],
            item[1]["_created_at"],
            str(item[1]["id"]),
        ),
    ):
        outcome = outcomes.get(claim_id)
        if outcome is None or outcome["_evidence_at"] <= snapshot["_evidence_at"]:
            continue
        categorical, _ = _validated_scores(outcome, claims[claim_id])
        if categorical is not None:
            numerator += categorical
            denominator += 1
        elif outcome["outcome"] == "unresolved":
            unresolved_count += 1
        elif outcome["outcome"] == "unverifiable":
            unverifiable_count += 1
        cases.append(
            {
                "claim_id": claim_id,
                "contrarian_snapshot_id": str(snapshot["id"]),
                "contrarian_as_of": _iso(snapshot["_evidence_at"]),
                "resolution_id": str(outcome["id"]),
                "outcome": str(outcome["outcome"]),
                "outcome_as_of": _iso(outcome["_evidence_at"]),
                "categorical_score": categorical,
                "corpus_release_id": context["release_id"],
            }
        )
    later_outcome_count = len(cases)
    return {
        "observation_count": len(observations),
        "later_accepted_outcome_count": later_outcome_count,
        "pending_later_outcome_count": len(observations) - later_outcome_count,
        "numerator": numerator,
        "denominator": denominator,
        "rate": _mean(numerator, denominator),
        "unresolved_count": unresolved_count,
        "unverifiable_count": unverifiable_count,
        "score_excluded_count": unresolved_count + unverifiable_count,
        "corpus_release_id": context["release_id"],
        "as_of": _iso(cutoff),
        "cases": cases,
    }


def _accepted_context(conn: sqlite3.Connection) -> dict[str, Any]:
    for surface, required_columns in _REQUIRED_SURFACES.items():
        available = _surface_columns(conn, surface)
        if not available:
            raise AccuracyUnavailableError(
                f"required accepted surface {surface!r} is unavailable"
            )
        missing = required_columns - available
        if missing:
            raise AccuracyUnavailableError(
                f"accepted surface {surface!r} is missing columns: "
                + ", ".join(sorted(missing))
            )
    releases = _fetch_dicts(
        conn,
        """
        SELECT id, release_version, manifest_sha256, promoted_at
        FROM current_accepted_corpus_releases
        ORDER BY release_version DESC, id
        """,
    )
    if len(releases) != 1:
        raise AccuracyUnavailableError(
            "accuracy requires exactly one current accepted corpus release; "
            f"found {len(releases)}"
        )
    release = releases[0]
    return {
        "release_id": str(release["id"]),
        "release_version": int(release["release_version"]),
        "manifest_sha256": str(release["manifest_sha256"]),
        "promoted_at": str(release["promoted_at"]),
    }


def _validated_scores(
    outcome: Mapping[str, Any], claim: Mapping[str, Any]
) -> tuple[float | None, float | None]:
    value = str(outcome.get("outcome"))
    if value not in _CATEGORICAL_SCORES:
        raise AccuracyDataError(
            f"outcome resolution {outcome.get('id')} has invalid outcome {value!r}"
        )
    categorical = _CATEGORICAL_SCORES[value]
    stored_categorical = outcome.get("categorical_score")
    if not _same_optional_number(stored_categorical, categorical):
        raise AccuracyDataError(
            f"outcome resolution {outcome.get('id')} has inconsistent categorical_score"
        )

    probability = claim.get("forecast_probability")
    if probability is None or value not in {"true", "false"}:
        brier = None
    else:
        probability = _probability(
            probability, f"claim {claim.get('id')} forecast_probability"
        )
        observed = 1.0 if value == "true" else 0.0
        brier = (probability - observed) ** 2
    if not _same_optional_number(outcome.get("brier_score"), brier):
        raise AccuracyDataError(
            f"outcome resolution {outcome.get('id')} has inconsistent brier_score"
        )
    return categorical, brier


def _surface_columns(conn: sqlite3.Connection, name: str) -> frozenset[str]:
    object_row = conn.execute(
        """
        SELECT name FROM sqlite_master WHERE name = ? AND type IN ('view', 'table')
        UNION ALL
        SELECT name FROM sqlite_temp_master WHERE name = ? AND type IN ('view', 'table')
        LIMIT 1
        """,
        (name, name),
    ).fetchone()
    if object_row is None:
        return frozenset()
    return frozenset(str(row[1]) for row in conn.execute(f'PRAGMA table_info("{name}")'))


def _fetch_dicts(
    conn: sqlite3.Connection,
    sql: str,
    parameters: Iterable[Any] = (),
) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, tuple(parameters))
    columns = [str(item[0]) for item in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _cutoff(value: str | None) -> dt.datetime:
    parsed = _optional_time(value or now_iso(), "as_of")
    assert parsed is not None
    return parsed


def _optional_time(value: str | None, field: str) -> dt.datetime | None:
    if value is None:
        return None
    parsed = parse_datetime(str(value))
    if parsed is None:
        raise ValueError(f"{field} must be an ISO-8601 date or timestamp")
    return parsed.astimezone(UTC)


def _row_time(row: Mapping[str, Any], field: str, kind: str) -> dt.datetime:
    parsed = parse_datetime(str(row.get(field) or ""))
    if parsed is None:
        raise AccuracyDataError(
            f"{kind} {row.get('id')} has invalid {field}: {row.get(field)!r}"
        )
    return parsed.astimezone(UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def _probability(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise AccuracyDataError(f"{field} must be a number") from exc
    if not math.isfinite(result) or result < 0.0 or result > 1.0:
        raise AccuracyDataError(f"{field} must be between 0 and 1")
    return result


def _same_optional_number(left: Any, right: float | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    try:
        numeric = float(left)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and math.isclose(
        numeric, right, rel_tol=1e-12, abs_tol=1e-12
    )


def _mean(numerator: float | int, denominator: int) -> float | None:
    return float(numerator) / denominator if denominator else None


def _positive_integer(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if result < 1 or result != value:
        raise ValueError(f"{field} must be a positive integer")
    return result


__all__ = [
    "AccuracyDataError",
    "AccuracyUnavailableError",
    "DEFAULT_MINIMUM_RESOLVED_CLAIMS",
    "person_accuracy_history",
    "person_accuracy_rankings",
    "person_contrarian_success",
]
