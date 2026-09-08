import pytest
from research_factory import signal_desk_full_event_v5 as v5
from research_factory import signal_desk_full_event_v4 as v4
from test_signal_desk_full_event_v4 import fixture, SOURCE


def value():
    v = fixture(); v["schema_version"] = v5.VERSION
    for e in v["events"]: e["context_evidence"] = []
    return v


def test_explicit_context_is_grounded_without_changing_voice_authority():
    v = value(); context = "Alex:"; start = SOURCE.index(context)
    v["events"][0]["context_evidence"] = [{"text": context, "start": start, "end": start+len(context), "purpose": "antecedent"}]
    v5.validate(v, source=SOURCE, window_id="dev")
    assert len(v["events"]) == 1 and not v5.receipt()["gold_accepted"]
    v["voice_bindings"][0]["voice_surface"] = "Someone Else"
    with pytest.raises(ValueError): v5.validate(v, source=SOURCE, window_id="dev")


@pytest.mark.parametrize("mutation", ["absent", "offset", "duplicate", "spoken_duplicate"])
def test_invalid_context_rejected(mutation):
    v=value(); e=v["events"][0]
    c={"text":"Alex:","start":0,"end":5,"purpose":"antecedent"};e["context_evidence"]=[c]
    if mutation == "absent": del e["context_evidence"]
    elif mutation == "offset": c["end"] += 1
    elif mutation == "duplicate": e["context_evidence"].append(dict(c))
    else: c.update(text=e["evidence_text"],start=e["evidence_start"],end=e["evidence_end"])
    with pytest.raises(ValueError): v5.validate(v, source=SOURCE, window_id="dev")


def test_empty_window_and_frozen_v4_unchanged():
    v=value();v.update(events=[],voice_bindings=[],window_disposition="no_records")
    v5.validate(v,source=SOURCE,window_id="dev")
    assert "context_evidence" not in v4.schema()["properties"]["events"]["items"]["properties"]


def test_other_speakers_context_does_not_extend_this_speakers_corridor():
    v=value(); source=SOURCE+"\nJordan: Are you sure?"
    start=len(SOURCE)+1
    corridor=dict(v["voice_bindings"][0]["corridor"])
    v["events"][0]["context_evidence"]=[{"text":source[start:],"start":start,"end":len(source),"purpose":"question"}]
    v5.validate(v,source=source,window_id="dev")
    assert v["voice_bindings"][0]["corridor"]==corridor
    assert v["voice_bindings"][0]["voice_surface"]=="Alex"
    # Moving the same cross-speaker text into the attributed evidence cannot
    # bypass the old corridor guard simply because v5 supports context.
    e=v["events"][0];e["context_evidence"]=[]
    e.update(evidence_text=source[start:],evidence_start=start,evidence_end=len(source))
    with pytest.raises(ValueError,match="corridor must cover"):
        v5.validate(v,source=source,window_id="dev")
