from __future__ import annotations

import copy

import pytest

from research_factory import true_north_prior_adoption as prior


def candidate(candidate_id: str = "cand-1") -> dict:
    return {
        "candidate_id": candidate_id,
        "speaker": {"name": "Host Name"},
        "reported_actor": {"name": "Quoted Org"},
        "actor_name": "Host Name",
        "claim_text": "The candidate prior is preserved verbatim.",
        "evidence_text": "Exact evidence bytes.",
        "evidence_start": 10,
        "evidence_end": 31,
        "candidate_concept": "candidate_prior",
        "certainty": "High Confidence",
        "claim_type": "Descriptive",
        "confidence": 1.4,
        "event_type": "frame_usage",
        "frame": "governance_frame",
        "stance": "Warning",
        "time_horizon": "Near Future",
    }


def decision(disposition: str = "retain") -> dict:
    return {
        "disposition": disposition,
        "reason_code": "stored_workhorse_decision",
    }


def test_retained_candidate_adopts_one_verbatim_prior_atom() -> None:
    row = candidate()
    output = prior.compose_prior_adoption(
        [row], {"cand-1": decision("retain")}
    )

    item = output["items"][0]
    assert item["disposition"] == "retain"
    assert len(item["atomic_claims"]) == 1
    atomic = item["atomic_claims"][0]
    assert atomic["claim_text"] == row["claim_text"]
    assert atomic["proposition_text"] == row["claim_text"]
    assert atomic["raw_speaker"] == "Host Name"
    assert atomic["reported_actor"] == "Quoted Org"


def test_candidate_enums_are_normalized_and_confidence_is_bounded() -> None:
    output = prior.compose_prior_adoption(
        [candidate()], {"cand-1": decision("revise")}
    )
    atomic = output["items"][0]["atomic_claims"][0]

    assert atomic["stance"] == "warning"
    assert atomic["certainty"] == "high"
    assert atomic["time_horizon"] == "near_future"
    assert atomic["polarity"] == "neutral"
    assert atomic["confidence"] == 1.0


@pytest.mark.parametrize("disposition", ["hold", "reject"])
def test_nonretained_candidate_has_no_atomic_claims(disposition: str) -> None:
    output = prior.compose_prior_adoption(
        [candidate()], {"cand-1": decision(disposition)}
    )
    assert output["items"][0]["atomic_claims"] == []


def test_absent_reported_actor_uses_actor_name_fallback() -> None:
    row = candidate()
    row["reported_actor"] = {"name": "none"}
    output = prior.compose_prior_adoption(
        [row], {"cand-1": decision()}
    )
    assert (
        output["items"][0]["atomic_claims"][0]["reported_actor"]
        == "Host Name"
    )


def test_exact_evidence_is_bound_from_candidate_not_decision() -> None:
    row = candidate()
    output = prior.compose_prior_adoption(
        [row],
        {
            "cand-1": {
                **decision(),
                "evidence_text": "invented",
                "evidence_start": 999,
                "evidence_end": 1000,
            }
        },
    )
    atomic = output["items"][0]["atomic_claims"][0]
    assert atomic["evidence_text"] == "Exact evidence bytes."
    assert atomic["evidence_start"] == 10
    assert atomic["evidence_end"] == 31


def test_input_candidate_is_not_mutated_by_validation() -> None:
    row = candidate()
    original = copy.deepcopy(row)
    prior.compose_prior_adoption([row], {"cand-1": decision()})
    assert row == original


def test_scope_mismatch_is_rejected() -> None:
    with pytest.raises(prior.PriorAdoptionError, match="scope mismatch"):
        prior.compose_prior_adoption(
            [candidate()], {"another": decision()}
        )
