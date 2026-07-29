from __future__ import annotations

import pytest

from research_factory import true_north
from research_factory.true_north_spark_conjunction import (
    _batch_source_packets,
    has_conjunction,
    validate_conjunction_output,
)


def _source(candidate_id: str, claim: str) -> dict:
    candidate = {
        "candidate_id": candidate_id,
        "proposed_claim_text": claim,
        "segment_id": "segment",
        "evidence_text": claim,
        "evidence_start": 0,
        "evidence_end": len(claim),
    }
    return {
        "instructions": ["frozen"],
        "input": {
            "candidates": [candidate],
            "stage_a_decisions": [
                {
                    "candidate_id": candidate_id,
                    "disposition": "retain",
                    "junk_reason": None,
                }
            ],
        },
    }


def test_selector_uses_exact_spaced_and_or_rule() -> None:
    assert has_conjunction("A and B")
    assert has_conjunction("A or B")
    assert not has_conjunction("Android systems")
    assert not has_conjunction("But this is separate")
    assert not has_conjunction("And this starts the sentence")


def test_batch_preserves_source_packets_and_scope() -> None:
    sources = [
        _source(f"candidate-{index}", f"A and B {index}")
        for index in range(3)
    ]

    batches = _batch_source_packets(sources)

    assert len(batches) == 1
    assert batches[0]["input"]["source_packets"] == sources
    assert (
        batches[0]["output_schema"]["properties"]["items"][
            "minItems"
        ]
        == 3
    )


def test_meta_packet_validator_enforces_frozen_adjudication_contract() -> None:
    packet = _batch_source_packets(
        [_source("candidate", "A and B")]
    )[0]
    valid = {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "items": [
            {
                "candidate_id": "candidate",
                "split": True,
                "atomic_claims": [
                    {"claim_text": "A"},
                    {"claim_text": "B"},
                ],
                "edit_reason": "compound_split",
            }
        ],
    }
    validate_conjunction_output(valid, packet)

    invalid = {
        **valid,
        "items": [
            {
                **valid["items"][0],
                "split": False,
            }
        ],
    }
    with pytest.raises(
        true_north.TrueNorthError,
        match="split=false",
    ):
        validate_conjunction_output(invalid, packet)
