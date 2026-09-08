import pytest
from scripts import pif_signal_desk_coding_tools_final_delta_review as r


def test_only_one_records_evidence_changes():
    if not (r.parent.OUT/'plan.json').exists():pytest.skip('private fixture absent')
    ps,fixed,proof=r.prepare(write=False);_,before,_=r.parent.prepare(write=False)
    assert [i for i,(a,b) in enumerate(zip(before['events'],fixed['events'])) if a!=b]==[9]
    e=fixed['events'][9];s=ps[0]['transcript_window']
    assert e['claim_text']==before['events'][9]['claim_text']
    assert e['evidence_text']==s[3018:3384] and e['context_evidence'][0]['text']==s[2844:3018]
    assert len(fixed['events'])==15 and len(ps[0]['candidates'])==1 and not proof['gold_accepted']


def test_requires_verified_parent_reviews(monkeypatch):
    if not (r.parent.OUT/'plan.json').exists():pytest.skip('private fixture absent')
    def reject(*args):raise ValueError('review provider mismatch')
    monkeypatch.setattr(r,'verify',reject)
    with pytest.raises(ValueError,match='provider'):r.prepare(write=False)
