import pytest
from research_factory.signal_desk_gold_shared_rubric import receipt, require_qualified


def test_draft_does_not_enable_or_modify_frozen_pipeline():
    r=receipt()
    assert not r["production_enabled"] and not r["frozen_prompts_changed"]
    assert r["status"]=="requires_qualification"


@pytest.mark.parametrize("qualification", [{}, {"passed": True}, {"rubric_sha256":"old","passed":True}])
def test_incomplete_or_stale_qualification_fails_closed(qualification):
    with pytest.raises(ValueError):require_qualified(qualification)
