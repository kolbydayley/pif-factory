import json
from research_factory.signal_desk_luxury_proposal import prepare
from scripts import pif_signal_desk_lineage_qualification as run


def test_no_invented_currency_or_identified_generic_consumer():
    fixed,proof,p=prepare();d=run.OUT/'calls'/p['window_id']/'A'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    assert len(fixed['events'])==15
    assert fixed['voice_bindings']==raw['voice_bindings']
    assert all(e['attribution']['transcript_voice'] is None for e in fixed['events'])
    assert '$' not in fixed['events'][8]['claim_text']
    assert fixed['events'][10]['attribution']['proposition_owner'] is None
    assert fixed['events'][12]['evidence_role']['role']=='research_limitation'
    assert fixed['events'][12]['evidence_role']['needs']=='audio_or_source'
    assert fixed['events'][12]['publishability_state']=='quarantined'
    assert all(e['publishability_state']=='uncertain' for i,e in enumerate(fixed['events']) if i!=12)
    assert [e['evidence_text'] for e in fixed['events']]==[e['evidence_text'] for e in raw['events']]
    assert not proof['gold_accepted']


def test_full_population_and_source_reach_independent_review():
    from scripts import pif_signal_desk_luxury_review as review
    ps=review.prepare(write=False);fixed,_,p=prepare()
    assert [e['event_id'] for q in ps for e in q['candidates']]==[e['event_id'] for e in fixed['events']]
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
