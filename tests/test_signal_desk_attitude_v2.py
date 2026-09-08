from copy import deepcopy
import pytest
from research_factory import signal_desk_attitude_v2 as v2
from research_factory import signal_desk_attitude_experiment as v1

SOURCE = "I think A is good but B is bad."


def span(text):
    start = SOURCE.index(text)
    return {"text": text, "start": start, "end": start + len(text)}


def packet():
    return {"decisions": [{"candidate_id": "c1", "proposition_status": "asserted", "epistemic": "hedged",
        "attitude": "indeterminate", "target": None, "evaluation_evidence": [], "status": "split_required",
        "rationale": "Two targets, one supplied candidate; not accepted labels.", "modality_evidence": [span("I think")],
        "target_components": [{"target": span("A"), "attitude": "positive", "evaluation_evidence": [span("A is good")]},
            {"target": span("B"), "attitude": "negative", "evaluation_evidence": [span("B is bad")]}]}]}


def check(p): return v2.validate(p, source=SOURCE, candidate_ids=["c1"])


def test_split_components_keep_single_candidate():
    p = packet(); check(p)
    assert len(p["decisions"]) == 1
    assert not v2.receipt()["qualified"]


@pytest.mark.parametrize("mutation", ["no_hedge", "inexact", "duplicate", "single", "collapsed", "not_split"])
def test_unsupported_detail_fails(mutation):
    p = packet(); r = p["decisions"][0]
    if mutation == "no_hedge": r["modality_evidence"] = []
    elif mutation == "inexact": r["modality_evidence"][0]["text"] = "Certain"
    elif mutation == "duplicate": r["target_components"][1] = deepcopy(r["target_components"][0])
    elif mutation == "single": r["target_components"].pop()
    elif mutation == "collapsed": r["attitude"] = "mixed"
    else: r["status"] = "needs_review"
    with pytest.raises(ValueError): check(p)


def test_parent_schema_unchanged():
    assert "modality_evidence" not in v1.schema()["properties"]["decisions"]["items"]["properties"]
    assert v2.receipt()["parent"] == v1.receipt()
