"""Deterministic detection of segment-overlap duplicate candidates (T2).

The transcript chunker emits **overlapping** segments, so an utterance sitting
on a boundary is extracted twice: once complete, and once as a
boundary-truncated fragment. Both copies then enter the candidate set as
independent candidates. Two of the junk escapes that stopped Task 4b are this
artifact, and no marginal-utility verifier can fix them by reading one
candidate at a time — the duplication is structural, not semantic.

This module finds those pairs with no model involvement:

- pair scope is restricted to **different segments with adjacent
  ``segment_index``** — that is the only place the chunker's overlap can put a
  duplicate. Repetition inside one segment is a different problem and is not
  claimed here;
- matching is containment on case- and whitespace-normalized evidence, so a
  truncated copy matches its complete original;
- the **complete copy is always the keeper**; only the shorter copy is reported
  as suppressible.

The relation is reported so downstream policy can treat the two cases
differently:

- ``exact_duplicate`` — identical evidence in both segments;
- ``boundary_truncated`` — the shorter copy is cut off (no terminal
  punctuation), the signature of a chunk boundary;
- ``contained`` — the shorter copy is a complete sentence that happens to be
  contained; weaker evidence, since a genuine shorter claim can nest inside a
  longer one.

This module changes no gate, no gold, and no scored run. It reports; it does
not suppress.

REFUTED AS A SUPPRESSION RULE — DO NOT WIRE IT IN
--------------------------------------------------
Measured against repaired development gold on 2026-07-29
(`docs/TRUE_NORTH_SEGMENT_OVERLAP_RESULTS.md`): the signal is **uncorrelated
with junk**.

- 73 duplicates flagged across 1,140 candidates; gold rejects only **6**.
  Precision **8.2%** — suppressing them would discard 65 gold-value candidates
  to catch 6 junk ones.
- Tightening with claim-text similarity makes it *worse*: at Jaccard ≥0.5
  precision is **0.0%**. Every gold-reject has claim text that *differs* from
  its keeper, and the near-identical-claim cases are all gold-value.

Why: overlapping segments legitimately yield **different propositions** from
the same evidence span. Shared evidence is not shared meaning, so evidence
containment cannot decide junk.

It does catch both Task 4b both-run escapes (`dev_d7f6bd87…` boundary-truncated,
`dev_fcec8903…` exact duplicate) — but as 2 of 6 true positives among 73 flags.
Use this module as a **corpus diagnostic** (how much boundary duplication
exists) only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

__all__ = [
    "SegmentOverlapError",
    "OverlapFinding",
    "SegmentOverlapResult",
    "MAX_SEGMENT_GAP",
    "find_segment_overlap_duplicates",
    "analyze_segment_overlap",
]


class SegmentOverlapError(ValueError):
    """Raised when a candidate cannot be evaluated for segment overlap."""


# The chunker overlaps consecutive segments only; a duplicate cannot appear
# more than one segment away from its original. Frozen constant.
MAX_SEGMENT_GAP = 1

_TERMINAL_PUNCTUATION = (".", "!", "?", '"', "”", ")", "…")


@dataclass(frozen=True)
class OverlapFinding:
    """One suppressible copy and the complete copy it duplicates."""

    duplicate_id: str
    keeper_id: str
    relation: str
    segment_gap: int


@dataclass(frozen=True)
class SegmentOverlapResult:
    findings: tuple[OverlapFinding, ...]
    duplicate_ids: tuple[str, ...]
    report: dict[str, Any]


def _normalized(text: Any) -> str:
    return " ".join(str(text or "").lower().split())


def _require(row: Mapping[str, Any], key: str, index: int) -> Any:
    if key not in row:
        raise SegmentOverlapError(
            f"candidate at index {index} lacks {key}"
        )
    return row[key]


def _relation(shorter: str, longer: str, raw_shorter: str) -> str:
    if shorter == longer:
        return "exact_duplicate"
    if not raw_shorter.strip().endswith(_TERMINAL_PUNCTUATION):
        return "boundary_truncated"
    return "contained"


def find_segment_overlap_duplicates(
    candidates: Iterable[Mapping[str, Any]],
) -> list[OverlapFinding]:
    """Return one finding per suppressible copy, deterministically ordered.

    Each candidate is reported at most once as a duplicate: a chain of nested
    copies resolves against the single longest keeper, never against several.
    """

    rows: Sequence[Mapping[str, Any]] = list(candidates)
    prepared: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        _require(row, "evidence_text", index)
        cid = str(_require(row, "candidate_id", index))
        normalized = _normalized(row.get("evidence_text"))
        if not normalized:
            # Blank evidence is a substring of everything; excluding it keeps
            # the containment test from matching the whole corpus.
            continue
        prepared.append(
            {
                "candidate_id": cid,
                "segment_id": str(row.get("segment_id") or ""),
                "segment_index": int(row.get("segment_index") or 0),
                "normalized": normalized,
                "raw": str(row.get("evidence_text")),
            }
        )

    # Longest first so a nested chain resolves against its complete original;
    # candidate_id breaks ties so the result never depends on input order.
    prepared.sort(key=lambda item: (-len(item["normalized"]), item["candidate_id"]))

    findings: list[OverlapFinding] = []
    claimed: set[str] = set()
    for keeper_pos, keeper in enumerate(prepared):
        if keeper["candidate_id"] in claimed:
            continue
        for other in prepared[keeper_pos + 1 :]:
            if other["candidate_id"] in claimed:
                continue
            if other["segment_id"] == keeper["segment_id"]:
                continue
            gap = abs(other["segment_index"] - keeper["segment_index"])
            if gap > MAX_SEGMENT_GAP or gap == 0:
                continue
            if other["normalized"] not in keeper["normalized"]:
                continue
            findings.append(
                OverlapFinding(
                    duplicate_id=other["candidate_id"],
                    keeper_id=keeper["candidate_id"],
                    relation=_relation(
                        other["normalized"], keeper["normalized"], other["raw"]
                    ),
                    segment_gap=gap,
                )
            )
            claimed.add(other["candidate_id"])

    findings.sort(key=lambda finding: finding.duplicate_id)
    return findings


def analyze_segment_overlap(
    candidates: Iterable[Mapping[str, Any]],
) -> SegmentOverlapResult:
    """Findings plus a per-relation accounting suitable for a run report."""

    rows = list(candidates)
    findings = tuple(find_segment_overlap_duplicates(rows))
    by_relation: dict[str, int] = {}
    for finding in findings:
        by_relation[finding.relation] = by_relation.get(finding.relation, 0) + 1
    return SegmentOverlapResult(
        findings=findings,
        duplicate_ids=tuple(finding.duplicate_id for finding in findings),
        report={
            "candidates": len(rows),
            "duplicates": len(findings),
            "by_relation": dict(sorted(by_relation.items())),
            "max_segment_gap": MAX_SEGMENT_GAP,
        },
    )
