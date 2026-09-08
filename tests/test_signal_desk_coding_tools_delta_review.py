import pytest
from scripts import pif_signal_desk_coding_tools_delta_review as r


def test_delta_preserves_eleven_supported_records_and_reviews_four():
    if not (r.parent.OUT/'plan.json').exists():pytest.skip('private fixture absent')
    ps,fixed,proof=r.prepare(write=False);_,baseline,_=r.parent.prepare(write=False)
    changed=[i for i,(a,b) in enumerate(zip(baseline['events'],fixed['events'])) if a!=b]
    assert changed==list(r.INDICES)
    assert sum(len(p['candidates']) for p in ps)==4 and len(fixed['events'])==15
    assert fixed['voice_bindings']==baseline['voice_bindings']
    assert fixed['events'][9]['evidence_start']==3018
    assert all('$' not in fixed['events'][i]['claim_text'] for i in (11,14))
    assert fixed['events'][13]['evidence_role']['scope']=='attributed_view'
    assert proof['unchanged_supported_records']==11 and not proof['gold_accepted']


def test_delta_requires_parent_actual_review_proof(monkeypatch):
    if not (r.parent.OUT/'plan.json').exists():pytest.skip('private fixture absent')
    def reject(*args):raise ValueError('approval provider mismatch')
    monkeypatch.setattr(r,'inspect_reviews',reject)
    with pytest.raises(ValueError,match='provider'):r.prepare(write=False)
