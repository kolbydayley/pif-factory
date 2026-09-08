from copy import deepcopy
import json
import pytest
from research_factory import signal_desk_flattened_b_recovery as recovery
from scripts import pif_signal_desk_flattened_b_delta_review as delta
from scripts import pif_signal_desk_flattened_b_review as parent


@pytest.mark.parametrize('missing',[True,False])
def test_final_surface_missing_or_rejected_blocks(monkeypatch,missing):
    from scripts import pif_signal_desk_flattened_b_surface_review as surface
    actual=recovery.verify
    def check(module,packets):
        if module is surface:
            if missing:raise ValueError('missing final approval')
            return [dict(event_id=packets[0]['candidates'][0]['event_id'],verdict='needs_correction')],[]
        return actual(module,packets)
    monkeypatch.setattr(recovery,'verify',check)
    _,_,p=parent.proposal();d=parent.run.OUT/'calls'/p['window_id']/'B'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError,match='missing final approval|not independently supported'):
        recovery.recover(raw,p)


def test_unreviewed_voice_and_other_records_rejected():
    baseline,_,_=parent.proposal();fixed,_,_=delta.proposal()
    recovery.unchanged_except_metadata(baseline,fixed)
    for mutate in ('voice','other'):
        bad=deepcopy(fixed)
        if mutate=='voice':bad['events'][1]['attribution']['transcript_voice']='invented'
        else:bad['events'][0]['claim_text']='changed'
        with pytest.raises(ValueError,match='unreviewed'):
            recovery.unchanged_except_metadata(baseline,bad)


@pytest.mark.parametrize('missing',[True,False])
def test_missing_or_rejected_delta_fails_closed(monkeypatch,missing):
    actual=recovery.verify
    def check(module,packets):
        if module is delta:
            if missing:raise ValueError('missing approval')
            return [dict(event_id=e['event_id'],verdict='needs_correction') for p in packets for e in p['candidates']],[]
        return actual(module,packets)
    monkeypatch.setattr(recovery,'verify',check)
    _,_,p=parent.proposal();d=parent.run.OUT/'calls'/p['window_id']/'B'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError,match='missing approval|not independently supported'):
        recovery.recover(raw,p)
