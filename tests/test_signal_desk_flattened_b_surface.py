from copy import deepcopy
from scripts import pif_signal_desk_flattened_b_surface_review as review
from scripts import pif_signal_desk_flattened_b_delta_review as parent


def test_one_claim_change_only_and_one_full_review_record():
    fixed,proof,p=review.proposal();baseline,_,_=parent.proposal()
    restored=deepcopy(fixed)
    restored['events'][3]['claim_text']=baseline['events'][3]['claim_text']
    assert restored==baseline
    ps=review.prepare(write=False)
    assert len(ps)==1 and ps[0]['candidates']==[fixed['events'][3]]
    assert ps[0]['transcript_window']==p['transcript_window']
    assert proof['gold_accepted'] is False
