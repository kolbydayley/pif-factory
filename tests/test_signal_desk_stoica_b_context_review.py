from scripts import pif_signal_desk_stoica_b_context_review as review


def test_only_antecedent_changes_with_full_source_review():
    old,_,_=review.parent.proposal();fixed,proof,p=review.proposal()
    span=fixed['events'][7]['context_evidence'][0]
    assert p['transcript_window'][span['start']:span['end']]==span['text']
    fixed['events'][7]['context_evidence']=old['events'][7]['context_evidence']
    assert fixed==old
    ps=review.prepare(write=False)
    assert [e['event_id'] for q in ps for e in q['candidates']]==[review.EVENT]
    assert ps[0]['transcript_window']==p['transcript_window']
    assert not proof['gold_accepted']
