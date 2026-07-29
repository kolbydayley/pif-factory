from __future__ import annotations

import pytest

from research_factory import true_north_gate_calibration as calibration


def test_mean_point_eight_recommends_point_seven_six() -> None:
    result = calibration.summarize_ceiling(
        [0.7, 0.8, 0.9],
        current_target=0.90,
    )
    assert result["mean"] == 0.8
    assert result["median"] == 0.8
    assert result["recommended_threshold"] == 0.76
    assert result["fraction_at_or_above_current_target"] == pytest.approx(
        1 / 3, abs=1e-6
    )


def test_ceiling_above_target_leaves_target_unchanged() -> None:
    result = calibration.summarize_ceiling(
        [0.96, 0.98],
        current_target=0.90,
    )
    assert result["mean"] == 0.97
    assert result["recommended_threshold"] == 0.90


def test_empty_ceiling_is_rejected() -> None:
    with pytest.raises(calibration.GateCalibrationError, match="must not be empty"):
        calibration.summarize_ceiling([], current_target=0.9)


def atomic(
    candidate_id: str,
    *,
    claim_text: str,
    reported_actor: str | None,
) -> dict:
    return {
        "candidate_id": candidate_id,
        "disposition": "retain",
        "reason_code": "fixture",
        "atomic_claims": [
            {
                "claim_text": claim_text,
                "claim_type": "descriptive",
                "raw_speaker": "Speaker",
                "reported_actor": reported_actor,
                "stance": "neutral",
                "certainty": "medium",
                "time_horizon": "present",
                "confidence": 0.8,
                "subject_text": "subject",
                "subject_type": "topic",
                "domain": "domain",
                "proposition_text": claim_text,
                "polarity": "positive",
                "position": "asserts",
                "evidence_text": "evidence",
                "evidence_start": 0,
                "evidence_end": 8,
            }
        ],
    }


def test_document_separates_matched_pair_ceiling_from_coupled_diagnostic() -> None:
    pass_a = {
        "one": atomic(
            "one",
            claim_text="AI systems require careful safety evaluation.",
            reported_actor="AI Lab",
        )
    }
    pass_b = {
        "one": atomic(
            "one",
            claim_text="AI systems need careful safety evaluation.",
            reported_actor=None,
        )
    }
    result = calibration.compute_ceiling_document(pass_a, pass_b)

    assert result["candidate_count"] == 1
    assert result["matched_atomic_pair_count"] == 1
    assert (
        result["ceilings"]["reported_actor_exactness"]["mean"]
        == 0.0
    )
    assert (
        result["coupled_diagnostics"][
            "reported_actor_campaign_micro_including_unmatched_atomics"
        ]
        == 0.0
    )
    assert result["proposed_faithfulness_gate"]["live_gate_changed"] is False
    assert (
        result["proposed_faithfulness_gate"]["required_companion_gate"][
            "threshold"
        ]
        == 0.02
    )


def test_pass_scopes_must_match() -> None:
    with pytest.raises(calibration.GateCalibrationError, match="same non-empty"):
        calibration.compute_ceiling_document(
            {"one": atomic("one", claim_text="a", reported_actor=None)},
            {"two": atomic("two", claim_text="b", reported_actor=None)},
        )
