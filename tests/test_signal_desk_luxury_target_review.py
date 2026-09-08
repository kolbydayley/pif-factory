from copy import deepcopy
from scripts import pif_signal_desk_luxury_target_review as review


def test_only_neutral_target_changes_and_full_record_is_reviewed():
    baseline, _, _ = review.parent.proposal()
    fixed, _, _ = review.proposal()
    assert fixed['events'][9]['attitude']['target'] is None
    restored = deepcopy(fixed)
    restored['events'][9]['attitude']['target'] = baseline['events'][9]['attitude']['target']
    assert restored == baseline
    packets = review.prepare(write=False)
    assert len(packets) == 1
    assert packets[0]['candidates'] == [fixed['events'][9]]
