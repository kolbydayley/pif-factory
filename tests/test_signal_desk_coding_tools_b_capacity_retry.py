from scripts import pif_signal_desk_coding_tools_b_capacity_retry as retry


def test_retry_one_unchanged_request_preserves_three_reviews():
    ps,receipt=retry.prepare(write=False)
    assert len(ps)==1
    assert ps[0]==retry.parent.prepare(write=False)[-1]
    assert len(receipt['preserved_reviews'])==3
    assert receipt['semantic_sample_unchanged'] is True
    assert receipt['original_charge_preserved'] is True
    assert receipt['gold_accepted'] is False
