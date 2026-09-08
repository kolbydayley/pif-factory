from scripts import pif_signal_desk_tsmc_c_ledger_retry as review


def test_retry_preserves_all_candidates_and_full_source():
    original,_=review.previous.prepare(write=False)
    old=next(p for p in original['ledger'] if p['packet_sha256']==review.FAILED)
    new=review.prepare(write=False)[0]
    assert new['candidates']==old['candidates']
    assert new['transcript_window']==old['transcript_window']
    assert new['schema_sha256']==old['schema_sha256']
    assert new['packet_sha256']!=old['packet_sha256']
    assert {c['decision_id'] for c in new['candidates']}==review.IDS
