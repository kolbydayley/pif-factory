from __future__ import annotations

from research_factory import true_north_c2_sol_adjudicator as c2


def _proposal(candidate_id: str, texts: list[str]) -> dict:
    return {
        "candidate_id": candidate_id,
        "atomic_claims": [{"claim_text": text} for text in texts],
    }


def test_c2_reservation_fits_declared_ceiling() -> None:
    assert 19 <= c2.MAX_CALLS
    assert (
        19 * c2.RESERVED_TOKENS_PER_ENVELOPE
        <= c2.MAX_TOKENS
    )
    assert c2.SOL_MODEL == "gpt-5.6-sol"
    assert c2.SOL_MODEL_LANE == "codex_subscription_ephemeral"


def test_adjudicator_statistics_separate_agreement_and_choices() -> None:
    pass_a = {
        "same": _proposal("same", ["S"]),
        "a": _proposal("a", ["A"]),
        "b": _proposal("b", ["B-old"]),
        "merge": _proposal("merge", ["M"]),
    }
    pass_b = {
        "same": _proposal("same", ["S"]),
        "a": _proposal("a", ["A-new"]),
        "b": _proposal("b", ["B"]),
        "merge": _proposal("merge", ["N"]),
    }
    selected = {
        "same": ["S"],
        "a": ["A"],
        "b": ["B"],
        "merge": ["M", "N"],
    }
    prior = {
        "same": ["S"],
        "a": ["A-new"],
        "b": ["B"],
        "merge": ["M"],
    }

    result = c2.adjudicator_statistics(
        pass_a=pass_a,
        pass_b=pass_b,
        selected=selected,
        prior_selected=prior,
    )

    assert result["proposals_identical"] == 1
    assert result["chose_a"] == 1
    assert result["chose_b"] == 1
    assert result["merged_union"] == 1
    assert result["contested_candidate_count"] == 3
    assert result["vs_phase_c_glm_adjudicator"][
        "exact_selection_disagreement_count"
    ] == 2


def test_codex_schema_adapter_removes_oneof_but_validator_stays_external() -> None:
    packet = {
        "schema_version": "pif_true_north_cheap_consensus_v1",
        "input": {
            "candidates": [
                {
                    "candidate_id": "c1",
                    "union_claim_texts": ["A.", "B."],
                },
                {
                    "candidate_id": "c2",
                    "union_claim_texts": ["C."],
                },
            ]
        },
    }

    schema = c2.codex_compatible_output_schema(packet)

    assert "oneOf" not in str(schema)
    item = schema["properties"]["items"]["items"]
    assert item["properties"]["candidate_id"]["enum"] == ["c1", "c2"]
    assert item["properties"]["selected_claim_texts"]["items"]["enum"] == [
        "A.",
        "B.",
        "C.",
    ]
