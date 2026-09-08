from scripts import pif_signal_desk_tsmc_c_review as review


def test_review_covers_every_record_disposition_and_addition():
    records,ledger,plan=review.prepare(write=False)
    fixed,_,p=review.proposal()
    assert [e['event_id'] for q in records for e in q['candidates']]==[e['event_id'] for e in fixed['records']['events']]
    ids=[e['decision_id'] for q in ledger for e in q['candidates']]
    assert len(ids)==len(set(ids))==29
    assert set(ids)=={f'input-{i:04d}' for i in range(27)}|{f'addition-{i:04d}' for i in range(2)}
    assert all(q['transcript_window']==p['transcript_window'] for q in records+ledger)
    assert all(len(q['candidates'])<=25 for q in records+ledger)
    assert not plan['qualified'] and not plan['gold_accepted']
