import pytest
from research_factory.signal_desk_attribution_experiment import validate, proposed_person_section, receipt


def identity(name, text, kind="person"):
    return {"surface_name": name, "kind": kind, "binding_span": {"start": 0, "end": len(text), "text": text}}


def record(voice=None, owner=None, relation="own_statement", mentions=None):
    return {"transcript_voice": voice, "proposition_owner": owner,
            "relation": relation, "mentioned_entities": mentions or []}


def test_known_quote_owner_does_not_require_guessed_narrator():
    source = 'Alice wrote: "Compute is scarce."'
    a = validate(record(owner=identity("Alice", source), relation="quoted_statement"), source=source)
    assert a["transcript_voice"] is None
    assert proposed_person_section(a, "Alice") == "direct_statements_and_quotations"


def test_report_is_not_direct_statement():
    source = "Alice reportedly expects delays."
    a = validate(record(owner=identity("Alice", source), relation="reported_statement"), source=source)
    assert proposed_person_section(a, "Alice") == "reported_views"


def test_mentioned_person_is_not_utterer():
    source = "Bob: Alice founded a company."
    voice = identity("Bob", source)
    a = validate(record(voice, voice, mentions=[{"surface_name": "Alice", "kind": "person"}]), source=source)
    assert proposed_person_section(a, "Alice") == "mentions"
    assert proposed_person_section(a, "Bob") == "direct_statements_and_quotations"


def test_unnamed_quote_owner_stays_unassigned():
    a = validate(record(relation="quoted_statement"), source="A VC said: grow faster.")
    assert proposed_person_section(a, "Alice") is None


def test_organization_report_does_not_create_person():
    source = "TechCrunch reports a launch."
    a = validate(record(owner=identity("TechCrunch", source, "organization"), relation="reported_statement"), source=source)
    assert proposed_person_section(a, "TechCrunch") is None


def test_organization_cannot_be_transcript_voice():
    source = "TechCrunch reports a launch."
    org = identity("TechCrunch", source, "organization")
    with pytest.raises(ValueError, match="must be a person"):
        validate(record(org, org), source=source)


def test_main_proposition_owner_cannot_be_mentioned_adviser():
    source = "Tom: I declined after Alice advised me."
    with pytest.raises(ValueError, match="different proposition owner"):
        validate(record(identity("Tom", source), identity("Alice", source)), source=source)


def test_binding_cannot_invent_source_or_name():
    with pytest.raises(ValueError, match="not exact source"):
        validate(record(owner=identity("Alice", "Alice said"), relation="quoted_statement"), source="Bob said")
    with pytest.raises(ValueError, match="absent from binding"):
        validate(record(owner=identity("Alice", "Bob said"), relation="quoted_statement"), source="Bob said")


def test_experiment_cannot_claim_qualification():
    r = receipt()
    assert not r["qualified"] and not r["production_enabled"] and not r["frozen_v2_compatible"]
    assert r["source_adjudication_required"]
