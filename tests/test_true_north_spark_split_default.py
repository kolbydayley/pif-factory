from __future__ import annotations

import copy

import pytest

from research_factory import true_north
from research_factory import true_north_spark_split_default as probe
from research_factory.true_north_input_split_default import (
    _flagged_packet,
)


def _semantic_packet(candidate_id: str) -> dict:
    candidate = {
        "candidate_id": candidate_id,
        "proposed_claim_text": "A rises and B falls",
        "segment_id": "segment",
        "evidence_text": "A rises and B falls.",
        "evidence_start": 0,
        "evidence_end": 20,
    }
    reference = {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "multipass_stage": "adjudication",
        "task": "frozen",
        "instructions": ["frozen"],
        "output_schema": true_north.multipass_adjudication_schema(
            [candidate_id]
        ),
        "input": {
            "episode": {"episode_id": "episode"},
            "segment": {"segment_id": "segment"},
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
    return _flagged_packet(reference, {candidate_id})


def test_pack_preserves_all_19_packets_in_14_envelopes() -> None:
    source = [
        {
            "packet_key": (
                f"episode-a/segment-{index:02d}"
                if index < 10
                else f"episode-b/segment-{index:02d}"
            ),
            "packet_sha256": str(index),
            "candidate_count": (index % 5) + 1,
            "packet": {"index": index},
        }
        for index in range(19)
    ]

    packed = probe.pack_provider_envelopes(source)

    assert len(packed) == 14
    assert sorted(
        row["packet_key"] for envelope in packed for row in envelope
    ) == sorted(row["packet_key"] for row in source)
    assert sum(len(envelope) == 2 for envelope in packed) == 5
    assert all(
        len(
            {
                row["packet_key"].split("/", 1)[0]
                for row in envelope
            }
        )
        == 1
        for envelope in packed
    )


def test_provider_validator_rechecks_each_original_packet() -> None:
    first = _semantic_packet("first")
    second = _semantic_packet("second")
    source = [
        {
            "packet_key": "episode/first",
            "packet_sha256": "a",
            "candidate_count": 1,
            "packet": first,
        },
        {
            "packet_key": "episode/second",
            "packet_sha256": "b",
            "candidate_count": 1,
            "packet": second,
        },
    ]
    envelope = probe._provider_packet("envelope", source)
    output = {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "items": [
            {
                "candidate_id": candidate_id,
                "split": True,
                "atomic_claims": [
                    {"claim_text": "A rises"},
                    {"claim_text": "B falls"},
                ],
                "edit_reason": "compound_split",
                "merge_reason": "none",
            }
            for candidate_id in ("first", "second")
        ],
    }

    probe.validate_provider_output(output, envelope)

    missing = copy.deepcopy(output)
    missing["items"].pop()
    with pytest.raises(
        probe.SparkSplitDefaultError, match="omitted"
    ):
        probe.validate_provider_output(missing, envelope)
