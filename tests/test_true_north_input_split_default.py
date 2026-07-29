from __future__ import annotations

import pytest

from research_factory import true_north
from research_factory.true_north_input_split_default import (
    InputSplitDefaultError,
    _flagged_packet,
    compound_flag,
    deterministic_clause_segmentation,
    validate_merge_adjudication,
)


def _reference_packet() -> dict:
    candidate = {
        "candidate_id": "candidate",
        "proposed_claim_text": "A rises and B falls, C stays flat",
        "segment_id": "segment",
        "evidence_text": "A rises and B falls, C stays flat.",
        "evidence_start": 0,
        "evidence_end": 35,
    }
    return {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "multipass_stage": "adjudication",
        "task": "frozen",
        "instructions": ["frozen"],
        "output_schema": true_north.multipass_adjudication_schema(
            ["candidate"]
        ),
        "input": {
            "episode": {"episode_id": "episode"},
            "segment": {"segment_id": "segment"},
            "candidates": [candidate],
            "stage_a_decisions": [
                {
                    "candidate_id": "candidate",
                    "disposition": "retain",
                    "junk_reason": None,
                }
            ],
        },
    }


def test_compound_screen_matches_conjunction_or_comma() -> None:
    assert compound_flag("A and B")
    assert compound_flag("A or B")
    assert compound_flag("A, B")
    assert not compound_flag("Android changes")


def test_segmentation_is_deterministic_and_evidence_aligned() -> None:
    clauses = deterministic_clause_segmentation(
        proposed_claim_text="A rises and B falls, C stays flat",
        evidence_text="A rises and B falls, C stays flat.",
    )

    assert [row["proposed_clause_text"] for row in clauses] == [
        "A rises",
        "B falls",
        "C stays flat",
    ]
    assert all(
        row["evidence_aligned_clause_text"]
        in "A rises and B falls, C stays flat."
        for row in clauses
    )


def test_merge_validator_binds_reason_to_count() -> None:
    packet = _flagged_packet(_reference_packet(), {"candidate"})
    valid = {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "items": [
            {
                "candidate_id": "candidate",
                "split": True,
                "atomic_claims": [
                    {"claim_text": "A rises"},
                    {"claim_text": "B falls and C stays flat"},
                ],
                "edit_reason": "compound_split",
                "merge_reason": "shared_subject_or_predicate",
            }
        ],
    }
    validate_merge_adjudication(valid, packet)

    invalid = {
        **valid,
        "items": [
            {
                **valid["items"][0],
                "merge_reason": "none",
            }
        ],
    }
    with pytest.raises(
        InputSplitDefaultError, match="merge_reason"
    ):
        validate_merge_adjudication(invalid, packet)
