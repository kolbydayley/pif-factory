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


def atomic_pair(
    candidate_id: str,
    *,
    claim_texts: list[str],
    reported_actor: str | None,
) -> dict:
    """A candidate carrying an arbitrary number of atomic claims."""
    base = atomic(
        candidate_id,
        claim_text=claim_texts[0],
        reported_actor=reported_actor,
    )
    template = base["atomic_claims"][0]
    base["atomic_claims"] = [
        {**template, "claim_text": text, "proposition_text": text}
        for text in claim_texts
    ]
    return base


def test_ceiling_document_reports_gate_denominator_alignment() -> None:
    """The live gate scores correct/max(predicted, gold); the ceiling must too.

    Pass A emits two atomics, pass B one, and the aligned pair agrees on speaker.
    Matched-pair agreement is therefore 1.0, while the live gate's own
    denominator yields 1/max(2, 1) = 0.5. A threshold derived from the
    matched-pair number is calibrated against a measurement the gate never
    makes, which is how an unpassable gate survives recalibration.
    """
    pass_a = {
        "one": atomic_pair(
            "one",
            claim_texts=[
                "AI systems require careful safety evaluation.",
                "Funding for interpretability research is increasing.",
            ],
            reported_actor="AI Lab",
        )
    }
    pass_b = {
        "one": atomic_pair(
            "one",
            claim_texts=["AI systems require careful safety evaluation."],
            reported_actor="AI Lab",
        )
    }
    result = calibration.compute_ceiling_document(pass_a, pass_b)

    alignment = result["gate_denominator_alignment"]
    assert alignment["live_gate_changed"] is False

    speaker = alignment["metrics"]["speaker_exactness"]
    assert speaker["matched_pair_ceiling_mean"] == 1.0
    assert speaker["coupled_ceiling_mean"] == 0.5
    assert speaker["live_target"] == 0.97
    assert speaker["gate_exceeds_coupled_ceiling"] is True
    assert speaker["recommended_threshold_on_gate_denominator"] == 0.46

    for metric in (
        "speaker_exactness",
        "reported_actor_exactness",
        "claim_text_faithfulness_proxy",
    ):
        assert metric in alignment["metrics"]


def test_gate_denominator_alignment_clears_when_gate_is_attainable() -> None:
    """Equal atomic counts and full agreement leave the gate inside the ceiling."""
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
            claim_text="AI systems require careful safety evaluation.",
            reported_actor="AI Lab",
        )
    }
    result = calibration.compute_ceiling_document(pass_a, pass_b)

    speaker = result["gate_denominator_alignment"]["metrics"][
        "speaker_exactness"
    ]
    assert speaker["matched_pair_ceiling_mean"] == 1.0
    assert speaker["coupled_ceiling_mean"] == 1.0
    assert speaker["gate_exceeds_coupled_ceiling"] is False


def test_pass_scopes_must_match() -> None:
    with pytest.raises(calibration.GateCalibrationError, match="same non-empty"):
        calibration.compute_ceiling_document(
            {"one": atomic("one", claim_text="a", reported_actor=None)},
            {"two": atomic("two", claim_text="b", reported_actor=None)},
        )
