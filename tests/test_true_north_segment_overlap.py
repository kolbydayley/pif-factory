from __future__ import annotations

import pytest

from research_factory import true_north_segment_overlap as overlap


def cand(
    cid: str,
    evidence: str,
    *,
    segment_index: int = 0,
    segment_id: str | None = None,
    claim_text: str = "a claim",
) -> dict:
    return {
        "candidate_id": cid,
        "segment_id": segment_id or f"seg_{segment_index}",
        "segment_index": segment_index,
        "evidence_text": evidence,
        "claim_text": claim_text,
    }


FULL = "Anthropic has spent years arguing that AI might soon be dangerous."
TRUNCATED = "Anthropic has spent years arguing that AI might soon be"


def test_boundary_truncated_copy_in_the_next_segment_is_a_duplicate() -> None:
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("a", FULL, segment_index=0),
            cand("b", TRUNCATED, segment_index=1),
        ]
    )
    assert len(findings) == 1
    assert findings[0].duplicate_id == "b"
    assert findings[0].keeper_id == "a"
    assert findings[0].relation == "boundary_truncated"


def test_the_complete_copy_is_always_the_keeper_regardless_of_order() -> None:
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("b", TRUNCATED, segment_index=1),
            cand("a", FULL, segment_index=0),
        ]
    )
    assert findings[0].duplicate_id == "b"
    assert findings[0].keeper_id == "a"


def test_identical_evidence_across_adjacent_segments_is_an_exact_duplicate() -> None:
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("a", FULL, segment_index=0),
            cand("b", FULL, segment_index=1),
        ]
    )
    assert len(findings) == 1
    assert findings[0].relation == "exact_duplicate"
    # Deterministic tie-break: lowest segment_index wins, then candidate_id.
    assert findings[0].keeper_id == "a"
    assert findings[0].duplicate_id == "b"


def test_containment_matching_ignores_case_and_whitespace() -> None:
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("a", FULL, segment_index=0),
            cand("b", "  anthropic   has SPENT years arguing", segment_index=1),
        ]
    )
    assert len(findings) == 1
    assert findings[0].duplicate_id == "b"


def test_same_segment_duplicates_are_not_segment_overlap() -> None:
    """Repetition inside one segment is a different problem; do not claim it."""
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("a", FULL, segment_index=0),
            cand("b", TRUNCATED, segment_index=0),
        ]
    )
    assert findings == []


def test_non_adjacent_segments_are_not_overlap_artifacts() -> None:
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("a", FULL, segment_index=0),
            cand("b", TRUNCATED, segment_index=4),
        ]
    )
    assert findings == []


def test_distinct_evidence_is_never_flagged() -> None:
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("a", FULL, segment_index=0),
            cand("b", "Regulators asked for a different disclosure.", segment_index=1),
        ]
    )
    assert findings == []


def test_a_complete_sentence_fragment_is_contained_not_boundary_truncated() -> None:
    """Containment alone is weaker evidence than a broken boundary."""
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("a", FULL + " Regulators agreed.", segment_index=0),
            cand("b", FULL, segment_index=1),
        ]
    )
    assert len(findings) == 1
    assert findings[0].relation == "contained"


def test_report_counts_by_relation_and_is_deterministic() -> None:
    candidates = [
        cand("a", FULL, segment_index=0),
        cand("b", TRUNCATED, segment_index=1),
        cand("c", "Regulators agreed on disclosure timing.", segment_index=2),
        cand("d", "regulators agreed on disclosure", segment_index=3),
    ]
    result = overlap.analyze_segment_overlap(candidates)
    assert result.report["candidates"] == 4
    assert result.report["duplicates"] == 2
    assert result.report["by_relation"]["boundary_truncated"] == 2
    assert result.duplicate_ids == ("b", "d")

    reversed_result = overlap.analyze_segment_overlap(list(reversed(candidates)))
    assert reversed_result.duplicate_ids == result.duplicate_ids
    assert reversed_result.report == result.report


def test_each_candidate_is_suppressed_at_most_once() -> None:
    """A chain A>B>C must not report B and C against multiple keepers."""
    candidates = [
        cand("a", FULL, segment_index=0),
        cand("b", TRUNCATED, segment_index=1),
        cand("c", "anthropic has spent years", segment_index=2),
    ]
    result = overlap.analyze_segment_overlap(candidates)
    assert len(set(result.duplicate_ids)) == len(result.duplicate_ids)
    assert "a" not in result.duplicate_ids


def test_missing_evidence_text_is_rejected() -> None:
    with pytest.raises(overlap.SegmentOverlapError, match="evidence_text"):
        overlap.find_segment_overlap_duplicates(
            [{"candidate_id": "a", "segment_index": 0}]
        )


def test_blank_evidence_is_ignored_not_matched_against_everything() -> None:
    findings = overlap.find_segment_overlap_duplicates(
        [
            cand("a", FULL, segment_index=0),
            cand("b", "   ", segment_index=1),
        ]
    )
    assert findings == []
