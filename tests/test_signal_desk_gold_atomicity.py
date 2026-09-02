import json

from research_factory.signal_desk_gold_atomicity import (
    ATOMICITY_REVIEW_WINDOW_IDS,
    evaluate_atomicity_review,
)


def _event(index, *, start=0, claim=None):
    return {
        "evidence_start": start,
        "evidence_end": start + 10,
        "claim_text": claim or f"distinct proposition {index}",
    }


def test_atomicity_review_flags_redundant_four_way_split(tmp_path):
    root = tmp_path
    (root / "C").mkdir()
    (root / "AUDIT").mkdir()
    for window_id in ATOMICITY_REVIEW_WINDOW_IDS:
        gold = [_event(0, start=0)]
        independent = [_event(0, start=0)]
        if window_id == "sdw_c793c28d8a642b4251af":
            gold = [
                _event(index, start=0, claim=f"the same redundant claim {index % 2}")
                for index in range(4)
            ]
        (root / "C" / f"{window_id}.json").write_text(
            json.dumps({"events": gold}), encoding="utf-8"
        )
        (root / "AUDIT" / f"{window_id}.json").write_text(
            json.dumps({"events": independent}), encoding="utf-8"
        )

    receipt = evaluate_atomicity_review(result_root=root)

    assert receipt["passed"] is False
    assert receipt["flagged_windows"] == ["sdw_c793c28d8a642b4251af"]


def test_atomicity_review_allows_distinct_claims_sharing_a_span(tmp_path):
    root = tmp_path
    (root / "C").mkdir()
    (root / "AUDIT").mkdir()
    for window_id in ATOMICITY_REVIEW_WINDOW_IDS:
        events = [_event(index, start=0) for index in range(4)]
        for turn in ("C", "AUDIT"):
            (root / turn / f"{window_id}.json").write_text(
                json.dumps({"events": events}), encoding="utf-8"
            )

    receipt = evaluate_atomicity_review(result_root=root)

    assert receipt["passed"] is True
