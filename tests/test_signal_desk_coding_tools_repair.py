import json
import pytest
from research_factory.signal_desk_coding_tools_repair import prepare
from scripts import pif_signal_desk_coding_tools_repair_review as review


def test_proposal_preserves_population_source_and_voices():
    if not (review.diagnosis.OUT/'plan.json').exists():pytest.skip('private fixture missing')
    p,fixed,proof=prepare()
    raw=json.loads((review.diagnosis.run.OUT/'calls'/review.diagnosis.WID/'A'/f'{review.diagnosis.PACKET}.output.json').read_text())
    assert fixed['voice_bindings']==raw['voice_bindings']
    assert [e['event_id'] for e in fixed['events']]==[e['event_id'] for e in raw['events']]
    assert [e['evidence_text'] for e in fixed['events']]==[e['evidence_text'] for e in raw['events']]
    assert fixed['events'][7]['attitude']['attitude']=='indeterminate'
    assert fixed['events'][7]['attitude']['epistemic']=='certain'
    assert all(fixed['events'][i]['publishability_state']=='quarantined' for i in (8,9))
    assert proof['records_before']==proof['records_after']==15 and not proof['gold_accepted']


def test_review_covers_full_proposal_with_untruncated_source():
    if not (review.diagnosis.OUT/'plan.json').exists():pytest.skip('private fixture missing')
    ps,fixed,proof=review.prepare(write=False)
    assert [e for p in ps for e in p['candidates']]==fixed['events']
    assert all(review.digest(p['transcript_window'])==review.diagnosis.SOURCE for p in ps)
    assert all(p['repair_proof_sha256']==review.digest(proof) for p in ps)


def test_proposal_requires_complete_verified_diagnosis(monkeypatch):
    monkeypatch.setattr(review.diagnosis,'status',lambda:{'all_reviews_verified':False})
    with pytest.raises(ValueError,match='diagnosis missing'):prepare()
