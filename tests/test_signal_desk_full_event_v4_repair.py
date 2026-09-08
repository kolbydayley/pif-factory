from copy import deepcopy
import pytest
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v4 import fixture, SOURCE


def test_explicit_repair_preserves_original_and_denominator():
    value = fixture(); value["events"][0]["evidence_start"] += 1
    original = deepcopy(value)
    fixed, receipt = propose(value, source=SOURCE, window_id="dev", expected_original_sha256=digest(value),
        replacements=[{"path": ["events", 0, "evidence_start"], "before": value["events"][0]["evidence_start"],
        "after": value["events"][0]["evidence_start"] - 1, "reason": "Explicit inspected source offset correction."}])
    assert value == original and fixed != original
    assert receipt["records_before"] == receipt["records_after"] == 1
    assert not receipt["gold_accepted"] and receipt["independent_review_required"]


def test_cannot_drop_bad_records_to_pass():
    value = fixture()
    with pytest.raises(ValueError, match="hide or rename"):
        propose(value, source=SOURCE, window_id="dev", expected_original_sha256=digest(value), replacements=[
            {"path": ["events"], "before": value["events"], "after": [], "reason": "Try dropping."}])


def test_expected_original_and_field_checks():
    value = fixture()
    with pytest.raises(ValueError, match="original response changed"):
        propose(value, source=SOURCE, window_id="dev", expected_original_sha256="wrong", replacements=[])
    with pytest.raises(ValueError, match="expected field changed"):
        propose(value, source=SOURCE, window_id="dev", expected_original_sha256=digest(value), replacements=[
            {"path": ["window_id"], "before": "wrong", "after": "dev", "reason": "Try changed field."}])
