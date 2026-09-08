import hashlib
import pytest
from research_factory import signal_desk_voice_continuity_v2 as v2
from research_factory import signal_desk_voice_continuity_experiment as v1

SOURCE = "Jobs fell. The following is an edited transcript. Brunson: I agree."


def span(text):
    start = SOURCE.index(text)
    return {"text": text, "start": start, "end": start + len(text)}


def packet(article=False):
    return {"source_sha256": hashlib.sha256(SOURCE.encode()).hexdigest(), "window_id": "dev",
        "decisions": [{"candidate_id": "c1", "voice_surface": None if article else "Brunson",
            "basis": "indeterminate" if article else "explicit_label",
            "identity_anchor": None if article else span("Brunson:"),
            "corridor": None if article else span("Brunson: I agree."),
            "excluded_voice_ranges": [], "rationale": "Source-bound experimental proposal.",
            "source_kind": "article_context" if article else "spoken_transcript",
            "source_kind_evidence": [span("The following is an edited transcript.")],
            "continuity_evidence": [] if article else [span("Brunson:")]}]}


def check(p, article=False):
    return v2.validate(p, source=SOURCE, window_id="dev", candidates={"c1": span("Jobs fell." if article else "I agree.")})


def test_article_is_not_unknown_spoken_person():
    check(packet(True), True)


def test_explicit_short_label_stays_short():
    p = packet(); check(p)
    p["decisions"][0]["voice_surface"] = "David Brunson"
    with pytest.raises(ValueError): check(p)


@pytest.mark.parametrize("mutation", ["no_continuity", "article_voice", "no_kind_evidence", "self_id_only", "outside", "inexact"])
def test_bad_voice_bindings_rejected(mutation):
    p = packet(); r = p["decisions"][0]
    if mutation == "no_continuity": r["continuity_evidence"] = []
    elif mutation == "article_voice": r["source_kind"] = "article_context"
    elif mutation == "no_kind_evidence": r["source_kind_evidence"] = []
    elif mutation == "self_id_only": r["basis"] = "self_identification_continuity"
    elif mutation == "outside": r["continuity_evidence"] = [span("Jobs fell.")]
    else: r["source_kind_evidence"][0]["text"] = "invented"
    with pytest.raises(ValueError): check(p)


def test_frozen_parent_not_mutated_and_no_approval():
    assert "continuity_evidence" not in v1.schema()["properties"]["decisions"]["items"]["properties"]
    assert v2.receipt()["parent"] == v1.receipt()
    assert not v2.receipt()["qualified"] and not v2.receipt()["production_enabled"]
