from __future__ import annotations

from research_factory.true_north_hallucination_ceiling import (
    _summarize_scores,
)


def _score(*, unmatched: bool = False, matched: bool = False) -> dict:
    matched_flags = (
        [
            {
                "severity": "hallucination",
                "kind": "predicted_field_absent_from_reference",
            }
        ]
        if matched
        else []
    )
    all_flags = list(matched_flags)
    if unmatched:
        all_flags.append(
            {
                "severity": "hallucination",
                "kind": "unmatched_predicted_claim",
            }
        )
    return {
        "hallucination_or_unsupported_proxy": {
            "flagged": bool(all_flags)
        },
        "alignment": [{"unsupported_field_flags": matched_flags}],
        "unsupported_field_flags": all_flags,
    }


def test_live_and_matched_only_hallucination_rates_are_separate() -> None:
    result = _summarize_scores(
        {
            "clean": _score(),
            "matched": _score(matched=True),
            "unmatched": _score(unmatched=True),
            "both": _score(unmatched=True, matched=True),
        }
    )

    assert result["live_proxy"]["rate"] == 0.75
    assert result["matched_pair_only_diagnostic"]["rate"] == 0.5
    assert result["gate_changed"] is False
    assert result["measurement_contract_finding_required"] is True

