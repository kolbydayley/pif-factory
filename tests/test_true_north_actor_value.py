from __future__ import annotations

import pytest

from research_factory.true_north_actor_value import (
    ActorValueError,
    SCHEMA_VERSION,
    _actor_packets,
    validate_actor_value_output,
)


def _packet() -> dict:
    return _actor_packets(
        [
            {
                "candidate_id": "candidate",
                "claim_index": 0,
                "episode_id": "episode",
                "segment_id": "segment",
                "claim_text": "Anthropic changed its policy.",
                "evidence_text": "Anthropic changed its policy.",
                "raw_speaker": "Speaker",
                "candidate_prior": "Other Lab",
            }
        ]
    )[0]


def test_literal_actor_or_null_is_valid() -> None:
    packet = _packet()
    for actor in ("Anthropic", None):
        validate_actor_value_output(
            {
                "schema_version": SCHEMA_VERSION,
                "items": [
                    {
                        "candidate_id": "candidate",
                        "claim_index": 0,
                        "reported_actor": actor,
                    }
                ],
            },
            packet,
        )


def test_non_substring_actor_is_schema_failure() -> None:
    with pytest.raises(
        ActorValueError, match="exact evidence substring"
    ):
        validate_actor_value_output(
            {
                "schema_version": SCHEMA_VERSION,
                "items": [
                    {
                        "candidate_id": "candidate",
                        "claim_index": 0,
                        "reported_actor": "OpenAI",
                    }
                ],
            },
            _packet(),
        )


def test_actor_output_scope_is_exact() -> None:
    with pytest.raises(
        ActorValueError, match="exactly cover"
    ):
        validate_actor_value_output(
            {
                "schema_version": SCHEMA_VERSION,
                "items": [],
            },
            _packet(),
        )


def test_actor_packets_are_capped_at_eight() -> None:
    rows = [
        {
            "candidate_id": f"candidate-{index}",
            "claim_index": 0,
            "episode_id": "episode",
            "segment_id": "segment",
            "claim_text": "Claim",
            "evidence_text": "Evidence",
            "raw_speaker": "Speaker",
            "candidate_prior": None,
        }
        for index in range(241)
    ]
    packets = _actor_packets(rows)
    assert len(packets) == 8
    assert sum(
        len(packet["input"]["claims"]) for packet in packets
    ) == 241
