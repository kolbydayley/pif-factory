import pytest
from research_factory import signal_desk_position_status_experiment as v1
from research_factory import signal_desk_position_status_v2 as v2


def test_separate_version_preserves_original_schema_and_receipt():
    assert v1.FAMILY_ID.endswith("v1") and v2.FAMILY_ID.endswith("v2")
    assert v1.schema() == v2.schema()
    assert v1.receipt()["sha256"] != v2.receipt()["sha256"]
    assert v2.receipt()["parent"] == v1.receipt()
    assert not v2.receipt()["qualified"]


@pytest.mark.parametrize("source,status", [
    ("Within a year competitors will catch up.", "actual_position"),
    ("If a company could take over the world, I would oppose it.", "actual_position"),
    ("People will say competition cannot work.", "anticipated_position"),
    ("Many people tweeted they were leaving.", "actual_position"),
])
def test_regression_proposals_not_semantic_acceptance(source, status):
    value = {"decisions": [{"candidate_id": "c1", "position_status": status,
        "source_evidence": [{"text": source, "start": 0, "end": len(source)}],
        "rationale": "Synthetic contrast for independent semantic testing."}]}
    v2.validate(value, source=source, candidate_ids=["c1"])
    assert not v2.receipt()["production_enabled"]
