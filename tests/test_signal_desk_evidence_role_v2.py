import pytest
from research_factory import signal_desk_evidence_role_v2 as v2
from research_factory import signal_desk_evidence_role_experiment as v1


def row(cid="c1", role="supporting_context"):
    return {"candidate_id": cid, "role": role, "decision": "boundary", "context_for": [],
        "context_parent_status": "missing" if role == "supporting_context" else "not_applicable",
        "scope": "attributed_view", "needs": "none", "rationale": "Retain caveat without inventing its missing parent."}


def test_missing_parent_is_retained_not_promoted():
    v2.validate({"decisions": [row()]}, ["c1"])
    assert not v2.receipt()["qualified"]


@pytest.mark.parametrize("mutation", ["invented", "resolved", "wrong_role", "blank"])
def test_missing_parent_cannot_hide_invalid_links(mutation):
    r = row()
    if mutation == "invented": r["context_for"] = ["nonexistent"]
    elif mutation == "resolved": r["decision"] = "proposed"
    elif mutation == "wrong_role": r["role"] = "substantive_claim"
    else: r["rationale"] = " "
    with pytest.raises(ValueError): v2.validate({"decisions": [r]}, ["c1"])


def test_real_parent_preserved():
    r = row(); r.update(context_parent_status="linked", context_for=["c2"], decision="proposed")
    v2.validate({"decisions": [r, row("c2", "substantive_claim")]}, ["c1", "c2"])
    assert "context_parent_status" not in v1.schema()["properties"]["decisions"]["items"]["properties"]
