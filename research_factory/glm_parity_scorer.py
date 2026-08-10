"""Parity scorer for provider comparison on the ai_discourse_v3_1 contract.

This exists because ``efficient_backtest._event_similarity`` / ``_match_events``
carry three biases that make cross-provider comparison unsound:

1. **Empty-vs-empty credit.** ``_token_jaccard`` returns 1.0 when both sides are
   empty, and the binary field comparisons return True when both are absent. A
   candidate that omits ``actor.name``, ``target.candidate_concept`` and the
   entity lists collects ~0.27 of free score against a gold event that also
   omits them, and 0.0 against one that populates them. Score therefore tracks
   field sparsity as much as agreement.

2. **Golden-order greedy matching.** ``_match_events`` walks gold events in
   index order and consumes the best still-free candidate. An early gold event
   can take a candidate that a later gold event fits better, inflating the
   "missed" count.

3. **A threshold calibrated against that biased distribution.** 0.48 was chosen
   under (1) and (2) and does not carry over.

The fixes here are (1) presence-renormalisation: fields absent on *both* sides
leave both the numerator and the denominator, so score is agreement over
*comparable* fields; and (2) optimal assignment.

Threshold is deliberately a required argument with no default. It is derived in
E4 from Codex-vs-Codex self-agreement, not chosen by hand — picking a number
that happens to improve a provider's score is the exact failure mode this module
is meant to remove.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

__all__ = [
    "FIELD_WEIGHTS",
    "ParityScorerError",
    "evaluator_sha256",
    "event_similarity",
    "match_events",
    "label_similarity",
    "aggregate",
]

_STOPWORDS = frozenset({"the", "and", "that", "with", "from", "this"})
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{1,}")

# Weights are the same relative weights efficient_backtest uses, so that results
# stay comparable field-for-field. What changes is that absent-on-both fields are
# dropped before normalising, rather than scoring 1.0.
FIELD_WEIGHTS: dict[str, float] = {
    "event_type": 0.15,
    "claim_type": 0.05,
    "evidence": 0.28,
    "claim_text": 0.25,
    "actor_name": 0.12,
    "target_concept": 0.08,
    "entities": 0.07,
}

_ENTITY_KEYS = ("model_names", "product_names", "organizations", "people")


class ParityScorerError(RuntimeError):
    """Raised when the scorer is asked to do something unsound."""


def _tokens(value: str) -> set[str]:
    return {token for token in _TOKEN_RE.findall(value.lower()) if token not in _STOPWORDS}


def _nested(value: Any, *path: str) -> Any:
    current: Any = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _string_list(value: Any) -> str:
    if isinstance(value, list):
        return " ".join(str(item) for item in value if item is not None)
    if value is None:
        return ""
    return str(value)


def _entities(event: dict[str, Any]) -> str:
    return " ".join(_string_list(event.get(key)) for key in _ENTITY_KEYS).strip()


def _field_values(event: dict[str, Any]) -> dict[str, str]:
    """Extract the comparable surface of an event as plain strings.

    An empty string means "absent". That is the presence signal the whole module
    turns on, so it is computed in exactly one place.
    """
    return {
        "event_type": str(event.get("event_type") or "").strip(),
        "claim_type": str(event.get("claim_type") or "").strip(),
        "evidence": str(event.get("evidence") or "").strip(),
        "claim_text": str(event.get("claim_text") or "").strip(),
        "actor_name": str(_nested(event, "actor", "name") or "").strip(),
        "target_concept": str(_nested(event, "target", "candidate_concept") or "").strip(),
        "entities": _entities(event),
    }


def _jaccard(left: str, right: str) -> float:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        # Absent-on-both never reaches here; it is excluded before scoring.
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _exact(left: str, right: str) -> float:
    return 1.0 if left == right else 0.0

_COMPARATORS: dict[str, Callable[[str, str], float]] = {
    "event_type": _exact,
    "claim_type": _exact,
    "evidence": _jaccard,
    "claim_text": _jaccard,
    "actor_name": _jaccard,
    "target_concept": _jaccard,
    "entities": _jaccard,
}


def event_similarity(left: dict[str, Any], right: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    """Presence-renormalised similarity in [0, 1], plus a per-field breakdown.

    Fields absent on both sides are excluded from numerator and denominator, so
    the result is agreement over the fields the two events actually populate.
    Returns 0.0 when the two events share no populated field — an event pair
    with nothing comparable is not a match, it is a non-comparison.
    """
    left_fields = _field_values(left)
    right_fields = _field_values(right)

    numerator = 0.0
    denominator = 0.0
    breakdown: dict[str, Any] = {}
    for field, weight in FIELD_WEIGHTS.items():
        lhs = left_fields[field]
        rhs = right_fields[field]
        if not lhs and not rhs:
            breakdown[field] = {"comparable": False, "similarity": None, "weight": 0.0}
            continue
        similarity = _COMPARATORS[field](lhs, rhs)
        numerator += weight * similarity
        denominator += weight
        breakdown[field] = {
            "comparable": True,
            "similarity": round(similarity, 6),
            "weight": weight,
        }

    if denominator <= 0.0:
        return 0.0, {"fields": breakdown, "comparable_weight": 0.0, "score": 0.0}

    score = numerator / denominator
    return score, {
        "fields": breakdown,
        "comparable_weight": round(denominator, 6),
        "score": round(score, 6),
    }


def _optimal_assignment(matrix: Sequence[Sequence[float]], *, threshold: float) -> list[tuple[int, int, float]]:
    """Maximum-weight one-to-one assignment, restricted to pairs >= threshold."""
    if not matrix or not matrix[0]:
        return []
    try:
        from scipy.optimize import linear_sum_assignment  # type: ignore[import-untyped]

        cost = [[-value for value in row] for row in matrix]
        row_idx, col_idx = linear_sum_assignment(cost)
        pairs = [
            (int(r), int(c), float(matrix[r][c]))
            for r, c in zip(row_idx, col_idx)
            if matrix[r][c] >= threshold
        ]
    except ImportError:  # pragma: no cover - exercised only where scipy is absent
        # Global-greedy fallback: strictly better than index-order greedy because
        # the highest-scoring pair anywhere wins first, but not guaranteed optimal.
        candidates = sorted(
            (
                (matrix[r][c], r, c)
                for r in range(len(matrix))
                for c in range(len(matrix[0]))
                if matrix[r][c] >= threshold
            ),
            key=lambda item: (-item[0], item[1], item[2]),
        )
        used_rows: set[int] = set()
        used_cols: set[int] = set()
        pairs = []
        for score, r, c in candidates:
            if r in used_rows or c in used_cols:
                continue
            used_rows.add(r)
            used_cols.add(c)
            pairs.append((r, c, float(score)))
    return sorted(pairs, key=lambda item: (item[0], item[1]))


def match_events(
    golden_events: Sequence[dict[str, Any]],
    candidate_events: Sequence[dict[str, Any]],
    *,
    threshold: float,
) -> list[tuple[int, int, float]]:
    """One-to-one optimal matching. ``threshold`` is required by design."""
    if not isinstance(threshold, (int, float)):
        raise ParityScorerError("threshold must be supplied explicitly; see E4 derivation")
    if not golden_events or not candidate_events:
        return []
    matrix = [
        [event_similarity(golden, candidate)[0] for candidate in candidate_events]
        for golden in golden_events
    ]
    return [
        (gi, ci, round(score, 6))
        for gi, ci, score in _optimal_assignment(matrix, threshold=float(threshold))
    ]


def _ratio(numerator: int, denominator: int) -> float | None:
    if not denominator:
        return None
    return round(numerator / denominator, 6)


def _f1(precision: float | None, recall: float | None) -> float | None:
    if precision is None or recall is None or (precision + recall) == 0:
        return None
    return round(2 * precision * recall / (precision + recall), 6)


def label_similarity(
    golden: dict[str, Any],
    candidate: dict[str, Any],
    *,
    threshold: float,
) -> dict[str, Any]:
    golden_events = [e for e in golden.get("discourse_events") or [] if isinstance(e, dict)]
    candidate_events = [e for e in candidate.get("discourse_events") or [] if isinstance(e, dict)]
    matched = match_events(golden_events, candidate_events, threshold=threshold)
    precision = _ratio(len(matched), len(candidate_events))
    recall = _ratio(len(matched), len(golden_events))
    return {
        "event_precision": precision,
        "event_recall": recall,
        "event_f1": _f1(precision, recall),
        "golden_events": len(golden_events),
        "candidate_events": len(candidate_events),
        "matched_events": len(matched),
        "match_scores": [score for _, _, score in matched],
    }


def aggregate(per_label: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Corpus-level rollup weighted the same way efficient_backtest weights it.

    Precision is weighted by candidate events, recall by golden events, and the
    corpus F1 is the harmonic mean of those two weighted averages -- not the mean
    of per-label F1s.
    """
    rows = list(per_label)
    golden_total = sum(int(row["golden_events"]) for row in rows)
    candidate_total = sum(int(row["candidate_events"]) for row in rows)
    matched_total = sum(int(row["matched_events"]) for row in rows)
    precision = _ratio(matched_total, candidate_total)
    recall = _ratio(matched_total, golden_total)
    return {
        "labels": len(rows),
        "golden_events": golden_total,
        "candidate_events": candidate_total,
        "matched_events": matched_total,
        "event_precision": precision,
        "event_recall": recall,
        "event_f1": _f1(precision, recall),
    }


def evaluator_sha256() -> str:
    """Digest of this module's source.

    ``extractor_gate._evaluate`` pins the evaluator and refuses a changed one, so
    editing this file requires a fresh ``initialize_gate`` -- never a mutated
    gate state.
    """
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
