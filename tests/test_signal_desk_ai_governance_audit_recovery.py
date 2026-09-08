import copy
import json
import pytest
from research_factory import signal_desk_ai_governance_audit_recovery as recovery
from scripts import pif_signal_desk_ai_governance_audit_review as review


def inputs():
    _,_,p=review.proposal();d=review.run.OUT/'calls'/p['window_id']/'AUDIT'
    return json.loads((d/f"{p['packet_sha256']}.output.json").read_text()),p


@pytest.mark.parametrize('missing',[True,False])
def test_missing_or_unsupported_delta_blocks(monkeypatch,missing):
    actual=recovery.verify
    def fail(module,packets):
        if module is review:return actual(module,packets)
        if missing:raise ValueError('missing independent approval')
        return ([{'event_id':e['event_id'],'verdict':'unresolved'} for p in packets for e in p['candidates']],[])
    monkeypatch.setattr(recovery,'verify',fail)
    with pytest.raises(ValueError,match='missing independent|not independently supported'):
        recovery.recover(*inputs())


def test_unreviewed_voice_pool_change_blocks():
    baseline,_,_=review.proposal();fixed=copy.deepcopy(baseline)
    fixed['voice_bindings']=[]
    with pytest.raises(ValueError,match='unreviewed content'):
        recovery.unchanged_approvals(baseline,fixed,{baseline['events'][1]['event_id']})
