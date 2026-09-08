from scripts import pif_signal_desk_flattened_b_review as parent
from scripts import pif_signal_desk_flattened_b_delta_review as review


def test_six_changes_preserve_every_quote_voice_and_remaining_record():
    raw,_,_=parent.proposal();fixed,proof,p=review.proposal()
    assert fixed['voice_bindings']==raw['voice_bindings']
    for n,(a,b) in enumerate(zip(raw['events'],fixed['events']),1):
        for key in ('event_id','attribution','voice_binding_id','evidence_text','evidence_start','evidence_end','publishability_state'):
            assert a[key]==b[key]
        if n not in (2,4,8,9,13,17):assert a==b
    ps=review.prepare(write=False)
    assert sum(len(q['candidates']) for q in ps)==6
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
    assert proof['gold_accepted'] is False
