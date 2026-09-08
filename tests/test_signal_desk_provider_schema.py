from copy import deepcopy
import pytest
from research_factory.signal_desk_provider_schema import lower
from research_factory.signal_desk_full_event_v4 import schema
from research_factory.signal_desk_evidence_role_v2 import validate


def test_lowering_preserves_canonical_and_only_removes_uniqueitems():
    original = schema(); saved = deepcopy(original); provider, receipt = lower(original)
    assert original == saved
    assert receipt["removed_provider_keywords"] == [["properties", "events", "items", "properties", "evidence_role", "properties", "context_for", "uniqueItems"]]
    provider["properties"]["events"]["items"]["properties"]["evidence_role"]["properties"]["context_for"]["uniqueItems"] = True
    assert provider == original and receipt["local_uniqueness_validation_mandatory"]


def test_duplicate_links_remain_locally_forbidden():
    r = {"candidate_id": "a", "role": "supporting_context", "decision": "proposed", "context_for": ["b", "b"],
        "context_parent_status": "linked", "scope": "attributed_view", "needs": "none", "rationale": "Duplicate."}
    b = dict(r, candidate_id="b", role="substantive_claim", context_for=[], context_parent_status="not_applicable")
    with pytest.raises(ValueError): validate({"decisions": [r, b]}, ["a", "b"])
