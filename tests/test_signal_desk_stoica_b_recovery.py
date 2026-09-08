import json
from copy import deepcopy
import pytest
from research_factory import signal_desk_stoica_b_recovery as recovery
from scripts import pif_signal_desk_stoica_b_review as review


def test_actual_full_review_chain_covers_all_twelve_records():
    _, _, p = review.proposal()
    d = review.run.OUT/'calls'/p['window_id']/'B'
    raw = json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    fixed, receipt = recovery.recover(raw, p)
    assert len(fixed['events']) == receipt['approved_records'] == 12
    assert not receipt['gold_accepted'] and not receipt['qualified']
    assert len(receipt['delta_reviews']) == 1


@pytest.mark.parametrize('missing',[True,False])
def test_missing_or_rejected_context_review_blocks(monkeypatch,missing):
    from scripts import pif_signal_desk_stoica_b_context_review as delta
    actual=recovery.verify
    def fail(module,ps):
        if module is delta:
            if missing:raise ValueError('missing approval')
            return [dict(event_id=delta.EVENT,verdict='needs_correction')],[]
        return actual(module,ps)
    monkeypatch.setattr(recovery,'verify',fail)
    _,_,p=review.proposal();d=review.run.OUT/'calls'/p['window_id']/'B'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError,match='missing approval|not independently supported'):
        recovery.recover(raw,p)


def test_unreviewed_voice_change_blocks():
    baseline,_,_=review.proposal();fixed=deepcopy(baseline);fixed['voice_bindings']=[]
    with pytest.raises(ValueError,match='unreviewed'):recovery.unchanged_except_context(baseline,fixed)
