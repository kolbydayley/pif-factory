import json
import pytest
from research_factory import signal_desk_stoica_c_recovery as recovery
from scripts import pif_signal_desk_stoica_c_review as first
from scripts import pif_signal_desk_stoica_c_delta_review as second


@pytest.mark.parametrize('missing',[True,False])
def test_fresh_ledger_missing_or_rejected_blocks(monkeypatch,missing):
    actual=recovery.verify
    def check(directory,p,**kwargs):
        if directory==second.OUT/'ledger':
            if missing:raise ValueError('missing ledger approval')
            return {'decisions':[dict(decision_id=d['decision_id'],verdict='needs_correction') for d in p['candidates']]},{}
        return actual(directory,p,**kwargs)
    monkeypatch.setattr(recovery,'verify',check)
    _,_,p=first.proposal();d=first.run.OUT/'calls'/p['window_id']/'C'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    with pytest.raises(ValueError,match='missing ledger approval|incomplete exact-content approval'):
        recovery.recover(raw,p)
