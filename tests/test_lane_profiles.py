import json

from research_factory.cheap_lane_adapters import draft_with_omission
from research_factory.lane_profiles import LANE_PROFILES, OMISSION_SUFFIX

TEMPLATE = "Extract from:\n{SEGMENT_TEXT}"


def _label(claims=None):
    return {
        "claims": claims or [],
        "entities": {"people": [], "organizations": [], "products": []},
        "topics": [],
        "summary": "s",
        "needs_review": False,
        "overall_confidence": 0.9,
    }


def _claim(text):
    return {"claim_text": text, "claim_type": "assessment",
            "evidence": text, "confidence": 0.9}


def test_profiles_cover_both_lanes_with_required_knobs():
    for lane in ("grok", "glm"):
        prof = LANE_PROFILES[lane]
        for key in ("concurrency", "omission_passes", "window_chars"):
            assert key in prof, f"{lane} missing {key}"
    # GLM live lane must stay at its promoted zero-omission config.
    assert LANE_PROFILES["glm"]["omission_passes"] == 0
    # Grok qualified WITH the omission pass; dropping it is a re-qualification.
    assert LANE_PROFILES["grok"]["omission_passes"] >= 1


def test_zero_passes_is_single_call():
    calls = []

    def fake(prompt):
        calls.append(prompt)
        return {"ok": True, "label": _label([_claim("a")]), "elapsed": 1.0}

    res = draft_with_omission(fake, TEMPLATE, "seg text", 0, OMISSION_SUFFIX)
    assert res["ok"] and len(calls) == 1
    assert [c["claim_text"] for c in res["label"]["claims"]] == ["a"]


def test_omission_pass_merges_new_claims_and_lists_priors():
    calls = []

    def fake(prompt):
        calls.append(prompt)
        if len(calls) == 1:
            return {"ok": True, "label": _label([_claim("first")]), "elapsed": 1.0}
        return {"ok": True, "label": _label([_claim("missed")]), "elapsed": 1.0}

    res = draft_with_omission(fake, TEMPLATE, "seg text", 1, OMISSION_SUFFIX)
    assert [c["claim_text"] for c in res["label"]["claims"]] == ["first", "missed"]
    assert "- first" in calls[1]              # prior claims listed
    assert "OMISSION AUDIT" in calls[1]
    assert "seg text" in calls[1]             # segment still present
    assert res["elapsed"] == 2.0


def test_failed_omission_pass_keeps_first_label():
    calls = []

    def fake(prompt):
        calls.append(prompt)
        if len(calls) == 1:
            return {"ok": True, "label": _label([_claim("first")]), "elapsed": 1.0}
        return {"ok": False, "label": None, "elapsed": 5.0, "error": "timeout"}

    res = draft_with_omission(fake, TEMPLATE, "seg text", 1, OMISSION_SUFFIX)
    assert res["ok"]
    assert [c["claim_text"] for c in res["label"]["claims"]] == ["first"]


def test_empty_omission_result_stops_early():
    calls = []

    def fake(prompt):
        calls.append(prompt)
        if len(calls) == 1:
            return {"ok": True, "label": _label([_claim("first")]), "elapsed": 1.0}
        return {"ok": True, "label": _label([]), "elapsed": 1.0}

    res = draft_with_omission(fake, TEMPLATE, "seg text", 3, OMISSION_SUFFIX)
    assert len(calls) == 2  # empty second pass short-circuits passes 2-3
    assert [c["claim_text"] for c in res["label"]["claims"]] == ["first"]


def test_first_pass_failure_propagates():
    def fake(prompt):
        return {"ok": False, "label": None, "elapsed": 2.0, "error": "boom"}

    res = draft_with_omission(fake, TEMPLATE, "seg text", 1, OMISSION_SUFFIX)
    assert not res["ok"] and res["error"] == "boom"
