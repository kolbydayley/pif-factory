"""needs_review drain — Kolby's approved ruling, 2026-08-10.

`docs/NEEDS_REVIEW_ANALYSIS_20260810.md` established that ~76% of the 8,341
flagged labels are input-condition disclosure (inferred speaker turns,
caption/ASR quality, segment overlap) or importance flags, not output defects.
The approved semantics:

- advisory families  -> ``advisory_acknowledged`` (bulk, deterministic);
- ``local_draft_bootstrap`` -> ``superseded_bootstrap``;
- real recall-loss families stay actionable for the bounded sampled drain;
- unclassified free text and missing reasons are **kept**, never silently
  cleared.

Resolutions are append-only rows in ``label_review_resolutions``. The
canonical ``labels`` rows are never mutated, so the drain is fully reversible
by deleting rows for a given ``method``.
"""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from .util import now_iso, stable_id

__all__ = [
    "ensure_review_resolution_schema",
    "classify_review_reason",
    "apply_bulk_resolutions",
    "actionable_backlog",
]


# Order matters: defect patterns are checked before advisory patterns so a
# reason mentioning both stays a defect (fail toward keeping review).
_DEFECT_PATTERNS = (
    re.compile(r"^Removed generated (items|entities)", re.IGNORECASE),
    re.compile(r"^Deterministic exact-evidence repair", re.IGNORECASE),
    re.compile(r"^Quarantined \d+ legacy", re.IGNORECASE),
    re.compile(r"^Structural salvage dropped", re.IGNORECASE),
    re.compile(r"^Exact-evidence validation pruned", re.IGNORECASE),
    re.compile(r"^All generated evidence spans failed", re.IGNORECASE),
    re.compile(r"validator_rejected", re.IGNORECASE),
)

_ADVISORY_PATTERNS = (
    # Speaker attribution/turns inferred because the transcript lacks them.
    re.compile(r"speaker[^.]*inferred", re.IGNORECASE),
    re.compile(r"inferred[^.]*speaker", re.IGNORECASE),
    # Caption/ASR/transcript source-quality disclosure.
    re.compile(r"\b(caption|captions|ASR|transcript)\b", re.IGNORECASE),
    # Segment-boundary overlap disclosure.
    re.compile(r"\boverlap", re.IGNORECASE),
    # Importance flags, alone or comma-joined with other tags.
    re.compile(r"^high_impact_signal(\s*,.*)?$", re.IGNORECASE),
    re.compile(r"^mixed_page$", re.IGNORECASE),
    re.compile(r"^mixed_source_context(\s*,.*)?$", re.IGNORECASE),
)

_BOOTSTRAP_PATTERN = re.compile(r"^local_draft_bootstrap$", re.IGNORECASE)


def classify_review_reason(reason: Any) -> str | None:
    """Classify one review_reason under the approved ruling.

    Returns ``"defect"``, ``"advisory"``, ``"bootstrap"``, ``"unclassified"``
    (recognized as text but matching no family — kept in the backlog), or
    ``None`` for an absent reason.
    """

    if reason is None:
        return None
    text = str(reason).strip()
    if not text:
        return None
    for pattern in _DEFECT_PATTERNS:
        if pattern.search(text):
            return "defect"
    if _BOOTSTRAP_PATTERN.match(text):
        return "bootstrap"
    for pattern in _ADVISORY_PATTERNS:
        if pattern.search(text):
            return "advisory"
    return "unclassified"


def ensure_review_resolution_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS label_review_resolutions (
          id TEXT PRIMARY KEY,
          label_id TEXT NOT NULL UNIQUE,
          resolution TEXT NOT NULL CHECK(resolution IN (
            'advisory_acknowledged', 'superseded_bootstrap',
            'cleared', 'corrected', 'quarantined'
          )),
          reason_class TEXT NOT NULL,
          review_reason_excerpt TEXT,
          method TEXT NOT NULL,
          resolved_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_review_resolutions_resolution
          ON label_review_resolutions(resolution)
        """
    )


def apply_bulk_resolutions(
    conn: sqlite3.Connection,
    *,
    method: str,
) -> dict[str, Any]:
    """Apply the deterministic bulk resolutions to every unresolved flagged
    label. Idempotent; labels rows are never written."""

    ensure_review_resolution_schema(conn)
    report = {
        "method": method,
        "resolved_advisory": 0,
        "resolved_bootstrap": 0,
        "kept_defect": 0,
        "kept_unclassified": 0,
        "kept_no_reason": 0,
        "already_resolved_skipped": 0,
        "already_clean_skipped": 0,
    }
    resolved_at = now_iso()
    rows = conn.execute(
        """
        SELECT labels.id AS label_id,
               json_extract(labels.output_json, '$.review_reason') AS reason,
               resolutions.id AS existing
        FROM labels
        LEFT JOIN label_review_resolutions AS resolutions
          ON resolutions.label_id = labels.id
        WHERE labels.needs_review = 1
        ORDER BY labels.id
        """
    ).fetchall()
    for row in rows:
        if row["existing"] is not None:
            report["already_resolved_skipped"] += 1
            continue
        family = classify_review_reason(row["reason"])
        if family is None:
            report["kept_no_reason"] += 1
            continue
        if family == "defect":
            report["kept_defect"] += 1
            continue
        if family == "unclassified":
            report["kept_unclassified"] += 1
            continue
        resolution = (
            "advisory_acknowledged"
            if family == "advisory"
            else "superseded_bootstrap"
        )
        conn.execute(
            """
            INSERT INTO label_review_resolutions
              (id, label_id, resolution, reason_class,
               review_reason_excerpt, method, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_id("review_resolution", row["label_id"], prefix="lrr_"),
                row["label_id"],
                resolution,
                family,
                str(row["reason"] or "")[:200],
                method,
                resolved_at,
            ),
        )
        key = (
            "resolved_advisory"
            if family == "advisory"
            else "resolved_bootstrap"
        )
        report[key] += 1
    conn.commit()
    return report


def actionable_backlog(conn: sqlite3.Connection) -> dict[str, int]:
    """Flagged labels with no resolution — the queue the sampled drain owns."""

    ensure_review_resolution_schema(conn)
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN resolutions.id IS NULL THEN 1 ELSE 0 END) AS actionable,
          SUM(CASE WHEN resolutions.id IS NOT NULL THEN 1 ELSE 0 END) AS resolved
        FROM labels
        LEFT JOIN label_review_resolutions AS resolutions
          ON resolutions.label_id = labels.id
        WHERE labels.needs_review = 1
        """
    ).fetchone()
    return {
        "actionable": int(row["actionable"] or 0),
        "resolved_total": int(row["resolved"] or 0),
    }
