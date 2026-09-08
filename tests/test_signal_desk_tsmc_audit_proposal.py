import json
from research_factory.signal_desk_tsmc_audit_proposal import prepare
from scripts import pif_signal_desk_lineage_qualification as run


def test_audit_preserves_population_and_attribution_without_claiming_approval():
    fixed,proof,p=prepare();d=run.OUT/'calls'/p['window_id']/'AUDIT'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    assert len(fixed['events'])==15
    assert fixed['voice_bindings']==raw['voice_bindings']
    for old,new in zip(raw['events'],fixed['events']):
        assert old['event_id']==new['event_id']
        assert old['attribution']==new['attribution']
        assert old['publishability_state']==new['publishability_state']
    assert all(e['attribution']['transcript_voice'] is None for e in fixed['events'][:3])
    assert 'probably' in fixed['events'][4]['claim_text']
    assert 'consulted salespeople' in fixed['events'][6]['claim_text']
    assert 'enabled' not in fixed['events'][11]['claim_text']
    assert not proof['gold_accepted']


def test_all_fifteen_audit_records_receive_full_source_review():
    from scripts import pif_signal_desk_tsmc_audit_review as review
    ps=review.prepare(write=False);fixed,_,p=prepare()
    assert [e['event_id'] for q in ps for e in q['candidates']]==[e['event_id'] for e in fixed['events']]
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
