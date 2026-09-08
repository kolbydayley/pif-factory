import pytest
from scripts import pif_signal_desk_tsmc_capacity_continuation as retry
from research_factory.signal_desk_review_capacity_continuation import prepare


def test_exact_remaining_requests_and_preserved_prefix():
    ps,lineage=retry.prepare(write=False)
    assert ps==retry.parent.prepare(write=False)[2:]
    assert len(lineage['preserved_reviews'])==2
    assert lineage['cooldown_seconds']==600
    assert lineage['same_semantic_sample'] and lineage['original_charge_preserved']


def test_wrong_failure_is_not_retried():
    with pytest.raises(ValueError):
        prepare(retry.parent,failed_sha='unknown',cooldown_seconds=600,output_root=retry.OUT,write=False)
