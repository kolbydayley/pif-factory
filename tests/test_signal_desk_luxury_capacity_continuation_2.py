import json
from scripts import pif_signal_desk_luxury_capacity_continuation_2 as retry
from research_factory.signal_desk_rubric_reference_packets import digest


def test_second_retry_preserves_both_failures_and_doubles_backoff():
    ps, receipt = retry.prepare(write=False)
    old, prior = retry.previous.prepare(write=False)
    assert ps == old == retry.previous.original.prepare(write=False)
    assert receipt['cooldown_seconds'] == 2*prior['cooldown_seconds'] == 1200
    assert receipt['capacity_failures_preserved'] == 2
    assert receipt['prior_capacity_lineage_sha256'] == digest(prior)
    assert receipt['preserved_reviews'] == []
    assert not receipt['gold_accepted']
    side = json.loads((retry.previous.OUT/f"{ps[0]['packet_sha256']}.sidecar.json").read_text())
    assert side['output_sha256'] is None
