from __future__ import annotations

import pytest

from research_factory import true_north_cheap_consensus as consensus


def _proposal(candidate_id: str, texts: list[str]) -> dict:
    return {
        "candidate_id": candidate_id,
        "split": len(texts) > 1,
        "edit_reason": "compound_split" if len(texts) > 1 else "none",
        "atomic_claims": [{"claim_text": text} for text in texts],
    }


def test_adjudication_packet_exposes_only_ab_union() -> None:
    source = {
        "input": {
            "episode": {"episode_id": "ep"},
            "segment": {"segment_id": "seg"},
            "candidates": [
                {
                    "candidate_id": "c1",
                    "evidence_text": "A and B.",
                    "proposed_claim_text": "A and B.",
                }
            ],
        }
    }
    packet = consensus.build_adjudication_packet(
        source_packet=source,
        pass_a={"c1": _proposal("c1", ["A and B."])},
        pass_b={"c1": _proposal("c1", ["A.", "B."])},
    )

    assert packet["input"]["candidates"][0]["union_claim_texts"] == [
        "A and B.",
        "A.",
        "B.",
    ]


def test_union_validator_rejects_third_structure() -> None:
    packet = {
        "input": {
            "candidates": [
                {
                    "candidate_id": "c1",
                    "union_claim_texts": ["A.", "B."],
                }
            ]
        }
    }
    with pytest.raises(
        consensus.CheapConsensusError,
        match="escaped the A/B union",
    ):
        consensus.validate_adjudication_output(
            {
                "schema_version": consensus.SCHEMA_VERSION,
                "items": [
                    {
                        "candidate_id": "c1",
                        "selected_claim_texts": ["C."],
                    }
                ],
            },
            packet,
        )


def test_declared_reservation_fits_phase_ceiling() -> None:
    assert 4 + 19 <= consensus.MAX_CALLS
    assert (
        4 * consensus.SOL_RESERVED_TOKENS_PER_ENVELOPE
        + 19 * consensus.GLM_RESERVED_TOKENS_PER_PACKET
        <= consensus.MAX_TOKENS
    )
