import pytest
from scripts.pif_signal_desk_gold_repair_proposals import proposed_patch, validate


def test_audit_repair_copies_only_disputed_fields_not_whole_event():
    case={"fields":["speaker_id"],"gold":{"speaker_id":"A","claim_text":"original"},"audit":{"speaker_id":"B","claim_text":"changed"}}
    assert proposed_patch(case,{"decision":"audit_supported","correction_json":"{}"}) == {"speaker_id":"B"}
    assert case["gold"]["claim_text"] == "original"


def test_restatement_is_not_a_repair():
    assert proposed_patch({"gold":{"stance":"neutral"}}, {"decision":"gold_supported","correction_json":'{"stance":"neutral"}'}) == {}


def test_claim_rewrites_are_rejected():
    with pytest.raises(ValueError):
        proposed_patch({"gold":{}},{"decision":"gold_supported","correction_json":'{"claim_text":"new"}'})


def test_context_request_must_explain_what_is_missing():
    with pytest.raises(ValueError):
        validate({"case_id":"a","verdict":"request_wider_context","rationale":"unclear","missing_context":""},"a")
