import json
from research_factory.signal_desk_ai_governance_audit_proposal import prepare
from scripts import pif_signal_desk_ai_governance_audit_review as review


def test_source_only_audit_full_population_and_unknown_voice_preserved():
    fixed,proof,p=prepare()
    d=review.run.OUT/'calls'/p['window_id']/'AUDIT'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    assert len(fixed['events'])==15
    assert fixed['voice_bindings']==raw['voice_bindings']
    for old,new in zip(raw['events'],fixed['events']):
        for field in ('event_id','claim_text','evidence_text','attribution','publishability_state','position','attitude'):
            if field in ('position','attitude'):continue  # only explicit numeric offsets below
            assert old[field]==new[field]
    assert all(c['path'][-1] in {'start','end','evidence_start','evidence_end','needs','scope','role','context_for','context_parent_status'} for c in proof['replacements'])
    assert all(e['attribution']['transcript_voice'] is None for e in fixed['events'][1:])
    assert fixed['events'][5]['position']['position_status']=='anticipated_position'
    assert fixed['events'][5]['evidence_role']['role']=='supporting_context'
    ps=review.prepare(write=False)
    assert sum(len(p['candidates']) for p in ps)==15
    assert all('author_a' not in p and 'author_b' not in p for p in ps)
    assert not proof['gold_accepted']
