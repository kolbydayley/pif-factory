from scripts import pif_signal_desk_stoica_c_review as parent
from scripts import pif_signal_desk_stoica_c_delta_review as review
from research_factory.signal_desk_rubric_reference_packets import digest


def test_record_population_and_full_new_lineage_review():
    original,_,_=parent.proposal()
    fixed,proof,p=review.proposal()
    assert fixed['records']['voice_bindings']==original['records']['voice_bindings']
    for a,b in zip(original['records']['events'],fixed['records']['events']):
        for key in ('event_id','attribution','voice_binding_id','evidence_text','evidence_start','evidence_end','publishability_state'):
            assert a[key]==b[key]
    assert fixed['records']['events'][4]['evidence_role']['role']=='supporting_context'
    assert len(proof['ledger_changes'])==2
    records,ledger,plan=review.prepare(write=False)
    assert sum(len(q['candidates']) for q in records)==5
    assert sum(len(q['candidates']) for q in ledger)==27
    assert all(q['transcript_window']==p['transcript_window'] for q in records+ledger)
    assert all(q['system_sha256']==digest(review.LEDGER_SYSTEM) for q in ledger)
    assert plan['gold_accepted'] is False
