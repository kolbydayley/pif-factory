from scripts import pif_signal_desk_tsmc_c_delta_review as review


def test_all_changed_and_unverified_items_receive_full_source_review():
    selected, plan = review.prepare(write=False)
    fixed, _, packet = review.proposal()
    assert [c['event_id'] for p in selected['records'] for c in p['candidates']] == [review.EVENT]
    assert {c['decision_id'] for p in selected['ledger'] for c in p['candidates']} == review.LINEAGE_IDS
    affected = {f'input-{i:04d}' for i,r in enumerate(fixed['input_dispositions'])
                if review.EVENT in r['output_event_ids']}
    assert affected <= review.LINEAGE_IDS
    assert affected == {'input-0012', 'input-0013'}
    assert all(p['transcript_window'] == packet['transcript_window']
               for ps in selected.values() for p in ps)
    assert plan['full_records'] == 15 and plan['full_lineage_items'] == 29
    assert not plan['gold_accepted'] and not plan['qualified']
