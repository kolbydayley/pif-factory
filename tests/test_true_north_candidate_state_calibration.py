from __future__ import annotations

import pytest

from research_factory import true_north_candidate_state_calibration as calibration


def item(candidate_id: str, disposition: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "disposition": disposition,
        "atomic_claims": [],
    }


def test_uses_live_three_class_macro_f1_not_raw_disposition_agreement() -> None:
    pass_a = {
        "one": item("one", "retain"),
        "two": item("two", "revise"),
        "three": item("three", "reject"),
        "four": item("four", "hold"),
    }
    pass_b = {
        "one": item("one", "revise"),
        "two": item("two", "retain"),
        "three": item("three", "reject"),
        "four": item("four", "hold"),
    }

    result = calibration.compute_candidate_state_calibration(pass_a, pass_b)

    assert result["measured_ceiling"] == 1.0
    assert result["diagnostics"]["raw_disposition_exact_agreement"] == 0.5
    assert result["diagnostics"]["value_state_exact_agreement"] == 1.0
    assert result["calibration_rule"]["recommended_threshold"] == 0.9
    assert result["finding"]["gate_exceeds_measured_ceiling"] is False


def test_ceiling_below_gate_produces_ruling_recommendation_without_change() -> None:
    pass_a = {
        "one": item("one", "retain"),
        "two": item("two", "reject"),
        "three": item("three", "hold"),
    }
    pass_b = {
        "one": item("one", "retain"),
        "two": item("two", "hold"),
        "three": item("three", "reject"),
    }

    result = calibration.compute_candidate_state_calibration(pass_a, pass_b)

    assert result["measured_ceiling"] == pytest.approx(1 / 3, abs=1e-6)
    assert result["calibration_rule"]["recommended_threshold"] == pytest.approx(
        0.293333, abs=1e-6
    )
    assert result["finding"]["gate_exceeds_measured_ceiling"] is True
    assert result["finding"]["gate_changed"] is False
    assert (
        result["finding"]["review_recommendation"]
        == "ruling_4_re_reference_to_recommended_threshold"
    )


def test_mismatched_scopes_are_rejected() -> None:
    with pytest.raises(
        calibration.CandidateStateCalibrationError,
        match="same non-empty",
    ):
        calibration.compute_candidate_state_calibration(
            {"one": item("one", "retain")},
            {"two": item("two", "retain")},
        )
