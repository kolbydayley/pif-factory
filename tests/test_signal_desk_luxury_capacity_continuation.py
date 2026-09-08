from scripts import pif_signal_desk_luxury_capacity_continuation as retry


def test_exact_same_two_packets_and_durable_cooldown():
    packets, receipt = retry.prepare(write=False)
    assert packets == retry.original.prepare(write=False)
    assert len(packets) == 2
    assert receipt['preserved_reviews'] == []
    assert receipt['cooldown_seconds'] == 600
    assert receipt['same_semantic_sample'] and receipt['original_charge_preserved']
    assert not receipt['gold_accepted'] and not receipt['applied']
