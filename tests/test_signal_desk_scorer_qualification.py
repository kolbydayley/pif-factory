import pytest

from research_factory.signal_desk_scorer_qualification import (
    ScorerQualificationError,
    qualification_receipt,
    select_qualification_cases,
)
from research_factory.signal_desk_gold_atomicity import ATOMICITY_REVIEW_WINDOW_IDS


def _event(index, *, speaker="A", stance="warning"):
    return {
        "speaker_id": speaker,
        "attribution_type": "direct_speech",
        "issue_label": f"issue-{index}",
        "stance": stance,
        "evidence_start": index * 20,
        "evidence_end": index * 20 + 10,
        "claim_text": f"claim number {index} changes work",
    }


def _rows():
    rows = []
    structures = ["speaker_turn", "paragraph", "flattened", "asr_diarized"]
    stress_ids = sorted(ATOMICITY_REVIEW_WINDOW_IDS)
    for window in range(20):
        events = [_event(index) for index in range(6)]
        predictions = [dict(event) for event in events]
        if window < len(stress_ids):
            # Several distinct claims share one exact evidence span.  The
            # scorer qualification must inspect this real split/merge shape.
            for index in (1, 2):
                events[index]["evidence_start"] = events[0]["evidence_start"]
                events[index]["evidence_end"] = events[0]["evidence_end"]
                predictions[index]["evidence_start"] = predictions[0]["evidence_start"]
                predictions[index]["evidence_end"] = predictions[0]["evidence_end"]
            predictions[2]["claim_text"] = "entirely unrelated proposition"
        # Cross-products supply real eligible and multiple ineligible families.
        if window % 2:
            predictions[0] = _event(0, speaker="B")
        if window % 3 == 0:
            predictions[1] = _event(1, stance="supportive")
        rows.append(
            {
                "window_id": stress_ids[window] if window < len(stress_ids) else f"w{window}",
                "transcript_structure": structures[window % len(structures)],
                "gold": {"events": events},
                "predicted": {"events": predictions},
            }
        )
    return rows


def test_selection_is_balanced_stratified_and_hash_frozen():
    selection = select_qualification_cases(_rows(), total=100)
    assert selection["case_count"] == 100
    assert selection["expected_balance"] == {"scorer_match": 50, "scorer_no_match": 50}
    assert selection["error_family_count"] >= 10
    assert selection["split_merge_stress_case_count"] >= 6
    assert len(selection["selection_sha256"]) == 64


def test_receipt_uses_adjudicated_expected_decisions():
    selection = select_qualification_cases(_rows(), total=100)
    adjudications = [
        {"case_id": case["case_id"], "expected_match": case["scorer_match"]}
        for case in selection["cases"]
    ]
    receipt = qualification_receipt(selection, adjudications)
    assert receipt["status"] == "qualified"
    assert receipt["agreement_lcb"] >= 0.97
    with pytest.raises(ScorerQualificationError, match="exactly cover"):
        qualification_receipt(selection, adjudications[:-1])
