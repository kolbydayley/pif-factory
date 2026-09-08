import hashlib

import pytest

from research_factory import signal_desk_voice_continuity_experiment as experiment

SOURCE = "I expect growth. Thanks for listening, I am Alex."


def span(text, source=SOURCE):
    start = source.index(text)
    return {"text": text, "start": start, "end": start + len(text)}


def packet():
    return {"source_sha256": hashlib.sha256(SOURCE.encode()).hexdigest(), "window_id": "dev1",
        "decisions": [{"candidate_id": "c1", "voice_surface": "Alex",
            "basis": "self_identification_continuity", "identity_anchor": span("I am Alex"),
            "corridor": span(SOURCE), "excluded_voice_ranges": [],
            "rationale": "Continuity proposed, independent review still required."}]}


def check(value):
    return experiment.validate(value, source=SOURCE, window_id="dev1", candidates={"c1": span("I expect growth.")})


def test_source_bound_backward_continuity_proposal():
    check(packet())
    assert experiment.receipt()["qualified"] is False
    assert experiment.receipt()["production_enabled"] is False


@pytest.mark.parametrize("mutation", ["source", "window", "name", "outside", "boundary", "boolean", "missing", "extra"])
def test_invalid_proposals(mutation):
    value = packet()
    row = value["decisions"][0]
    if mutation == "source":
        value["source_sha256"] = "stale"
    elif mutation == "window":
        value["window_id"] = "different"
    elif mutation == "name":
        row["voice_surface"] = "Someone Else"
    elif mutation == "outside":
        row["corridor"] = span("I am Alex")
    elif mutation == "boundary":
        row["excluded_voice_ranges"] = [span("Thanks for listening")]
    elif mutation == "boolean":
        row["corridor"]["start"] = False
    elif mutation == "missing":
        value["decisions"] = []
    else:
        row["accepted"] = True
    with pytest.raises(ValueError):
        check(value)


def test_indeterminate_cannot_guess_voice():
    value = packet()
    row = value["decisions"][0]
    row["basis"] = "indeterminate"
    with pytest.raises(ValueError):
        check(value)
    for field in ("voice_surface", "identity_anchor", "corridor"):
        row[field] = None
    check(value)


def test_name_occurrence_is_not_semantic_proof():
    # Structurally exact but knowingly unsound: Alex is mentioned, not speaking.
    source = "I expect growth. My guest is Alex."
    value = packet()
    value["source_sha256"] = hashlib.sha256(source.encode()).hexdigest()
    row = value["decisions"][0]
    row["identity_anchor"] = span("My guest is Alex", source)
    row["corridor"] = span(source, source)
    experiment.validate(value, source=source, window_id="dev1", candidates={"c1": span("I expect growth.", source)})
    assert experiment.receipt()["independent_identity_and_continuity_review_required"] is True
