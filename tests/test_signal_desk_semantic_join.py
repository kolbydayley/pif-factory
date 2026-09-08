import hashlib
import pytest
from research_factory.signal_desk_semantic_join import validate, research_routes, receipt

SOURCE = "Alex: People will say I oppose regulation."


def span(text):
    start = SOURCE.index(text)
    return {"text": text, "start": start, "end": start + len(text)}


def fixture():
    identity = {"surface_name": "Alex", "kind": "person", "binding_span": span("Alex:")}
    b = {"attribution": {"c1": {"transcript_voice": identity, "proposition_owner": None,
        "relation": "reported_statement", "mentioned_entities": []}},
        "evidence_role": {"decisions": [{"candidate_id": "c1", "role": "substantive_claim", "decision": "proposed",
            "context_for": [], "context_parent_status": "not_applicable", "scope": "attributed_view", "needs": "none", "rationale": "An imagined argument."}]},
        "position": {"decisions": [{"candidate_id": "c1", "position_status": "anticipated_position",
            "source_evidence": [span("People will say")], "rationale": "Not an observed opponent."}]},
        "voice": {"source_sha256": hashlib.sha256(SOURCE.encode()).hexdigest(), "window_id": "dev",
            "decisions": [{"candidate_id": "c1", "voice_surface": "Alex", "basis": "explicit_label",
                "identity_anchor": span("Alex:"), "corridor": span(SOURCE), "excluded_voice_ranges": [],
                "rationale": "Labeled turn.", "source_kind": "spoken_transcript",
                "source_kind_evidence": [span("Alex:")], "continuity_evidence": [span("Alex:")]}]},
        "attitude": {"decisions": [{"candidate_id": "c1", "proposition_status": "hypothetical", "epistemic": "certain",
            "attitude": "negative", "target": span("regulation"), "evaluation_evidence": [span("oppose regulation")],
            "status": "proposed", "rationale": "Imagined speaker's opposition, not Alex's endorsement.",
            "modality_evidence": [], "target_components": []}]}}
    return b


def check(b): return validate(b, source=SOURCE, window_id="dev", candidates={"c1": span("People will say I oppose regulation.")})


def test_join_preserves_population_and_anticipated_position_not_observed():
    b = fixture(); check(b)
    r = research_routes(b, source=SOURCE, window_id="dev", candidates={"c1": span("People will say I oppose regulation.")})
    assert len(r) == 1 and not r[0]["independent_claim_eligible_if_accepted"]
    assert r[0]["observed_person_path_if_independently_accepted"] is None
    assert r[0]["counted_source_breadth"] == 0 and not r[0]["accepted"]


@pytest.mark.parametrize("mutation", ["missing_family", "wrong_id", "invent_voice", "different_anchor", "own_owner"])
def test_cross_dimension_contradictions(mutation):
    b = fixture()
    if mutation == "missing_family": b.pop("position")
    elif mutation == "wrong_id": b["attitude"]["decisions"][0]["candidate_id"] = "c2"
    elif mutation == "invent_voice": b["attribution"]["c1"]["transcript_voice"] = None
    elif mutation == "different_anchor": b["attribution"]["c1"]["transcript_voice"]["binding_span"] = span("Alex")
    else: b["attribution"]["c1"]["relation"] = "own_statement"
    with pytest.raises(ValueError): check(b)


def test_no_family_is_qualified_by_join():
    assert not receipt()["qualified"] and not receipt()["production_enabled"]
    assert all(not r["qualified"] for r in receipt()["families"].values())


def test_named_hypothetical_owner_still_not_observed_voice():
    b = fixture()
    b["attribution"]["c1"]["proposition_owner"] = dict(b["attribution"]["c1"]["transcript_voice"])
    rows = research_routes(b, source=SOURCE, window_id="dev", candidates={"c1": span("People will say I oppose regulation.")})
    assert rows[0]["observed_person_path_if_independently_accepted"] is None


def test_promotion_never_routes_to_substantive_person_view():
    b = fixture(); b["attribution"]["c1"]["proposition_owner"] = dict(b["attribution"]["c1"]["transcript_voice"])
    b["position"]["decisions"][0]["position_status"] = "actual_position"
    b["evidence_role"]["decisions"][0]["role"] = "promotion_housekeeping"
    rows = research_routes(b, source=SOURCE, window_id="dev", candidates={"c1": span("People will say I oppose regulation.")})
    assert rows[0]["observed_person_path_if_independently_accepted"] is None
    assert not rows[0]["independent_claim_eligible_if_accepted"]
