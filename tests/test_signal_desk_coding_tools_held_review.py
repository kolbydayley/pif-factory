import copy
import json
import pytest
from scripts import pif_signal_desk_coding_tools_held_review as review


def fixture():
    if not (review.run.OUT/'plan.json').exists():pytest.skip('private development fixture absent')
    ps=review.prepare(write=False)
    d=review.run.OUT/'calls'/review.WID/'A'
    raw=json.loads((d/f'{review.PACKET}.output.json').read_text())
    return ps,raw


def test_diagnosis_preserves_every_invalid_record_and_full_source():
    ps,raw=fixture()
    assert [e for p in ps for e in p['candidates']]==raw['events']
    assert all(p['candidate_population']==15 and review.digest(p['transcript_window'])==review.SOURCE for p in ps)
    assert all(p['gold_accepted'] is False for p in ps)
    import tiktoken
    enc=tiktoken.get_encoding('o200k_base')
    for p in ps:
        assert len(p['candidates'])<=25
        assert len(enc.encode(review.SYSTEM+json.dumps(p,ensure_ascii=False)+json.dumps(review.run.previous.review.schema(p))))+1500<=12000


def test_special_diagnostic_path_cannot_accept_other_inputs():
    ps,raw=fixture();source=ps[0]['transcript_window']
    changed=copy.deepcopy(raw);changed['events'][0]['claim_text']='altered'
    for value,s,w in [(changed,source,review.WID),(raw,source+'x',review.WID),(raw,source,'other')]:
        with pytest.raises(ValueError,match='not the inspected'):review.inspected_invalid_input(value,source=s,window_id=w)


def test_standard_acceptance_validator_still_rejects_original():
    ps,raw=fixture()
    with pytest.raises(ValueError,match='inexact span'):
        review.run.previous.contract.validate(raw,source=ps[0]['transcript_window'],window_id=review.WID)
