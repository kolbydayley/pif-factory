import copy
import json
import pytest
from research_factory import signal_desk_stoica_recovery as recovery
from scripts import pif_signal_desk_stoica_review as review


def inputs():
    _,_,p=review.proposal()
    d=review.run.OUT/'calls'/p['window_id']/'A'
    return json.loads((d/f"{p['packet_sha256']}.output.json").read_text()),p


def test_missing_delta_blocks(monkeypatch):
    real=recovery.verify
    def absent(module,packets):
        if module is review:return real(module,packets)
        raise ValueError('missing delta')
    monkeypatch.setattr(recovery,'verify',absent)
    with pytest.raises(ValueError,match='missing delta'):recovery.recover(*inputs())


def test_unsupported_delta_blocks(monkeypatch):
    real=recovery.verify
    def unsupported(module,packets):
        if module is review:return real(module,packets)
        return ([{'event_id':e['event_id'],'verdict':'unresolved'} for p in packets for e in p['candidates']],[])
    monkeypatch.setattr(recovery,'verify',unsupported)
    with pytest.raises(ValueError,match='not independently supported'):recovery.recover(*inputs())


def test_voice_or_claim_mutation_cannot_reuse_approval():
    baseline,_,_=review.proposal()
    fixed=copy.deepcopy(baseline);fixed['events'][0]['claim_text']='unreviewed'
    with pytest.raises(ValueError,match='unreviewed content'):recovery.assert_attitude_only_delta(baseline,fixed)
