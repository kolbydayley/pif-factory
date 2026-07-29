from __future__ import annotations

import pytest

from research_factory.true_north_actor_two_stage import (
    ActorTwoStageError,
    SCHEMA_VERSION,
    _batch_rows,
    span_candidates,
    validate_emission_output,
    validate_value_output,
)


def _row() -> dict:
    return {
        "candidate_id": "candidate",
        "claim_index": 0,
        "episode_id": "episode",
        "segment_id": "segment",
        "claim_text": "Anthropic changed its policy.",
        "evidence_text": "Anthropic changed its policy.",
        "raw_speaker": "Speaker",
        "candidate_prior": "Other Lab",
    }


def test_emission_stage_returns_only_binary_decision() -> None:
    packet = _batch_rows([_row()], stage="emission")[0]
    validate_emission_output(
        {
            "schema_version": SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "candidate",
                    "claim_index": 0,
                    "emit": True,
                }
            ],
        },
        packet,
    )


def test_value_stage_requires_literal_substring() -> None:
    packet = _batch_rows([_row()], stage="value")[0]
    validate_value_output(
        {
            "schema_version": SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": "candidate",
                    "claim_index": 0,
                    "reported_actor": "Anthropic",
                }
            ],
        },
        packet,
    )
    with pytest.raises(
        ActorTwoStageError, match="exact evidence substring"
    ):
        validate_value_output(
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
            packet,
        )


def test_span_candidates_are_exact_nonbinding_hints() -> None:
    evidence = "Ramp says Anthropic and OpenAI changed."
    spans = span_candidates(evidence, "Anthropic")

    assert spans[0] == "Anthropic"
    assert all(span in evidence for span in spans)
