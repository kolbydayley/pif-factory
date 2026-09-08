from copy import deepcopy
import pytest
from research_factory import signal_desk_full_event_v4_review as review
from test_signal_desk_full_event_v4 import fixture, SOURCE


def packet(): return review.packets(fixture(), source=SOURCE, window_id="dev", token_count=lambda x: 1)[0]


def response():
    return {"empty_window_verdict": "not_applicable", "empty_window_rationale": "", "coverage_notes": "",
        "decisions": [{"event_id": "c1", "verdict": "supported", "correction_proposal": "", "rationale": "Imagined view stays distinct.",
            "source_quotes": ["People will say"]}]}


def test_full_source_bindings_and_population_preserved():
    p = packet()
    assert p["transcript_window"] == SOURCE and len(p["voice_bindings"]) == 1
    assert p["record_index"][0]["event_id"] == "c1"
    review.validate_review(response(), p)


@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate", "quote", "silent_correction", "empty_correction"])
def test_invalid_review_rejected(mutation):
    v = response(); row = v["decisions"][0]
    if mutation == "missing": v["decisions"] = []
    elif mutation == "extra": row["event_id"] = "new"
    elif mutation == "duplicate": v["decisions"].append(deepcopy(row))
    elif mutation == "quote": row["source_quotes"] = ["People said"]
    elif mutation == "silent_correction": row["correction_proposal"] = "Change ownership"
    else: row["verdict"] = "needs_correction"
    with pytest.raises(ValueError): review.validate_review(v, packet())


def test_no_truncation_when_oversized():
    with pytest.raises(ValueError): review.packets(fixture(), source=SOURCE, window_id="dev", token_count=lambda x: 12000)


def test_empty_requires_actual_verdict():
    v = fixture(); v.update(events=[], voice_bindings=[], window_disposition="no_records")
    p = review.packets(v, source=SOURCE, window_id="dev", token_count=lambda x: 1)[0]
    r = response(); r["decisions"] = []
    with pytest.raises(ValueError): review.validate_review(r, p)
    r.update(empty_window_verdict="missed_records", empty_window_rationale="Source is not empty.")
    review.validate_review(r, p)
    assert not review.receipt()["gold_accepted"]
