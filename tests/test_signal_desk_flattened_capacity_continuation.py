import pytest
from scripts import pif_signal_desk_flattened_capacity_continuation as retry
from research_factory.signal_desk_review_capacity_continuation import prepare


def test_only_final_original_packet_retried_after_cooldown():
    packets, receipt = retry.prepare(write=False)
    original = retry.parent.prepare(write=False)
    assert packets == original[-1:]
    assert len(receipt['preserved_reviews']) == 6
    assert receipt['parent_packets'] == [p['packet_sha256'] for p in original]
    assert receipt['cooldown_seconds'] == 300
    assert receipt['same_semantic_sample']
    assert receipt['original_charge_preserved']
    assert not receipt['gold_accepted'] and not receipt['applied']


def test_cannot_retry_completed_packet_or_skip_cooldown():
    first = retry.parent.prepare(write=False)[0]['packet_sha256']
    with pytest.raises(ValueError, match='not a no-output capacity failure'):
        prepare(retry.parent, failed_sha=first, cooldown_seconds=300,
                output_root=retry.OUT, write=False)
    with pytest.raises(ValueError, match='cooldown below minimum'):
        prepare(retry.parent, failed_sha=first, cooldown_seconds=299,
                output_root=retry.OUT, write=False)
