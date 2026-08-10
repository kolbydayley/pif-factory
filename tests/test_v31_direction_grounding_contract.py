from __future__ import annotations

import pytest

from research_factory.labels import (
    ValidationError,
    _validate_v31_metric_grounding,
    repair_label_output_for_submission,
)


def _event(direction: str, direction_evidence: str | None, evidence: str) -> dict:
    return {
        "claim_text": "A sufficiently long directional claim.",
        "evidence": evidence,
        "metric": {
            "value": None,
            "unit": None,
            "comparator": None,
            "direction": direction,
            "direction_evidence": direction_evidence,
            "raw_text": None,
        },
        "quality_flags": [],
    }


@pytest.mark.parametrize(
    ("direction", "phrase"),
    [
        ("increase", "grew"),
        ("decrease", "fell"),
        ("stable", "held steady"),
        ("mixed", "mixed"),
        ("unknown", "direction remains unclear"),
    ],
)
def test_explicit_exact_direction_evidence_is_accepted(
    direction: str, phrase: str
) -> None:
    _validate_v31_metric_grounding(
        0, _event(direction, phrase, f"Revenue {phrase} this quarter.")
    )


@pytest.mark.parametrize(
    ("direction", "phrase", "evidence", "failure"),
    [
        ("increase", "fell", "Revenue fell.", "wrong_direction"),
        ("stable", "profitable", "Revenue is profitable.", "stable_filler"),
        ("unknown", "number is unknown", "The number is unknown.", "unknown_filler"),
        ("increase", "ten million", "Revenue is ten million.", "no_directional_claim"),
    ],
)
def test_direction_failure_taxonomy_is_enforced(
    direction: str, phrase: str, evidence: str, failure: str
) -> None:
    with pytest.raises(ValidationError, match=failure):
        _validate_v31_metric_grounding(0, _event(direction, phrase, evidence))


def test_missing_direction_evidence_is_quarantined_non_destructively() -> None:
    original = _event("increase", None, "Revenue grew.")
    payload = {"discourse_events": [original]}
    quarantines: list[dict] = []

    repairs = repair_label_output_for_submission(
        "ai_discourse_v3_1",
        payload,
        segment_text="Revenue grew.",
        metric_quarantines=quarantines,
    )

    assert repairs == 1
    assert quarantines[0]["failed_rules"] == [
        "direction_evidence_not_evidence_substring"
    ]
    assert quarantines[0]["original_metric"]["direction"] == "increase"
    assert payload["discourse_events"][0]["metric"] == {
        "value": None,
        "unit": None,
        "comparator": None,
        "direction": "not_applicable",
        "direction_evidence": None,
        "raw_text": None,
    }


def test_not_applicable_direction_rejects_direction_evidence() -> None:
    with pytest.raises(ValidationError, match="direction_evidence_with_not_applicable"):
        _validate_v31_metric_grounding(
            0, _event("not_applicable", "grew", "Revenue grew.")
        )
