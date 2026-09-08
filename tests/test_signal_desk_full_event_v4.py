from copy import deepcopy
import pytest
from research_factory import signal_desk_full_event_v4 as v4
from research_factory import signal_desk_full_event_experiment as v3
from test_signal_desk_semantic_join import fixture as bundle_fixture, SOURCE


def fixture():
    bundle = bundle_fixture()
    text = "People will say I oppose regulation."
    event = {"event_id": "c1", "claim_text": "An imagined opponent objects to regulation.",
        "speech_act": "reported_position", "evidence_text": text, "evidence_start": SOURCE.index(text), "evidence_end": len(SOURCE),
        "attribution_confidence": .8, "issue_label": "Regulation", "issue_aliases": [], "publishability_state": "candidate",
        "attribution": bundle["attribution"]["c1"]}
    for key in v4.DIMENSIONS:
        event[key] = {k: deepcopy(v) for k, v in bundle[key]["decisions"][0].items() if k != "candidate_id"}
    return {"schema_version": v4.VERSION, "window_id": "dev", "window_disposition": "records_found", "events": [event]}


def check(v): return v4.validate(v, source=SOURCE, window_id="dev")


def test_composed_record_uses_one_id_and_no_old_stance():
    v = fixture(); check(v)
    assert "stance" not in v["events"][0]
    assert v4.counts(v, source=SOURCE, window_id="dev")["accepted_claims"] == 0


@pytest.mark.parametrize("mutation", ["accepted", "duplicate", "extra_id", "voice", "old_stance", "offset", "promotion"])
def test_invalid_composition_rejected(mutation):
    v = fixture(); e = v["events"][0]
    if mutation == "accepted": e["publishability_state"] = "accepted"
    elif mutation == "duplicate": v["events"].append(deepcopy(e))
    elif mutation == "extra_id": e["attitude"]["candidate_id"] = "other"
    elif mutation == "voice": e["voice"]["voice_surface"] = "Someone Else"
    elif mutation == "old_stance": e["stance"] = "supportive"
    elif mutation == "offset": e["evidence_start"] = True
    else: e["evidence_role"]["role"] = "promotion_housekeeping"
    with pytest.raises(ValueError): check(v)


def test_empty_not_dropped():
    v = fixture(); v["events"] = []; v["window_disposition"] = "no_records"; check(v)
    assert v4.counts(v, source=SOURCE, window_id="dev")["records"] == 0


def test_original_frozen_schema_unchanged():
    assert "stance" in v3.schema()["properties"]["events"]["items"]["properties"]
    assert not v4.receipt()["qualified"]
