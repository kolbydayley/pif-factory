import json
from copy import deepcopy
import pytest
from research_factory import signal_desk_flattened_audit_recovery as recovery
from scripts import pif_signal_desk_flattened_audit_review as review


def test_actual_complete_seventeen_record_chain():
    _,_,p=review.proposal();d=review.run.OUT/'calls'/p['window_id']/'AUDIT'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    fixed,receipt=recovery.recover(raw,p)
    assert len(fixed['events'])==receipt['approved_records']==17
    assert not receipt['gold_accepted'] and not receipt['qualified']


@pytest.mark.parametrize('missing',[True,False])
def test_missing_or_rejected_final_target_blocks(monkeypatch,missing):
    from scripts import pif_signal_desk_flattened_audit_target_review as final
    actual=recovery.verify
    def fail(module,ps):
        if module is final:
            if missing:raise ValueError('missing approval')
            return [dict(event_id='evt_09',verdict='needs_correction')],[]
        return actual(module,ps)
    monkeypatch.setattr(recovery,'verify',fail)
    _,_,p=review.proposal();d=review.run.OUT/'calls'/p['window_id']/'AUDIT'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError,match='missing approval|not independently supported'):
        recovery.recover(raw,p)


def test_unreviewed_voice_binding_change_blocks():
    from scripts import pif_signal_desk_flattened_audit_delta_review as delta
    baseline,_,_=review.proposal();fixed,_,_=delta.proposal();fixed['voice_bindings']=[]
    with pytest.raises(ValueError,match='unreviewed'):recovery.unchanged_except_metadata(baseline,fixed)
