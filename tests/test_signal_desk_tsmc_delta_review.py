from scripts import pif_signal_desk_tsmc_delta_review as delta


def test_only_one_claim_changes_and_full_population_is_preserved():
    baseline,_,source=delta.parent.proposal()
    packets,fixed,_=delta.prepare(write=False)
    assert len(fixed['events'])==14
    assert packets[0]['transcript_window']==source['transcript_window']
    assert len(packets)==1 and len(packets[0]['candidates'])==1
    assert 'enabled' not in fixed['events'][10]['claim_text']
    fixed['events'][10]['claim_text']=baseline['events'][10]['claim_text']
    assert fixed==baseline
