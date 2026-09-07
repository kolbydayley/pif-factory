from copy import deepcopy
import pytest
from scripts.pif_signal_desk_full_event_report import summarize
from research_factory.signal_desk_full_event_review import packets
from research_factory.signal_desk_full_event_experiment import VERSION


def fixture():
    value = {"schema_version": VERSION, "window_id": "w", "window_disposition": "no_consequential_claims", "events": []}
    ps = packets(value, source="Hello.", window_id="w", token_count=lambda s: 1)
    reviews = {ps[0]["packet_sha256"]: {"decisions": [], "empty_window_verdict": "supported_empty", "empty_window_rationale": "Greeting only."}}
    return ps, reviews, {"w": {r: deepcopy(value) for r in ("A", "B", "C", "AUDIT")}}, {"w": "paragraph"}


def test_all_roles_and_empty_denominator_remain():
    result = summarize(*fixture())
    assert result["windows"] == 1 and result["candidate_events"] == 0
    assert result["verdicts"]["supported_empty"] == 1
    assert result["by_role"]["AUDIT"]["empty_windows"] == 1
    assert len(result["comparisons"]) == 3 and all(c["both_empty"] for c in result["comparisons"])
    assert not result["gold_accepted"] and not result["gate_eligible"]


def test_missing_final_review_fails_closed():
    ps, rs, values, structures = fixture()
    with pytest.raises(ValueError, match="incomplete review"): summarize(ps, {}, values, structures)


def test_empty_window_with_missed_claims_stays_unresolved():
    ps, rs, values, structures = fixture(); next(iter(rs.values()))["empty_window_verdict"] = "missed_claims"
    assert summarize(ps, rs, values, structures)["unresolved_final_review_proposals"][0]["verdict"] == "missed_claims"


def test_missing_audit_role_fails_closed():
    ps, rs, values, structures = fixture(); del values["w"]["AUDIT"]
    with pytest.raises(ValueError, match="missing qualification role"): summarize(ps, rs, values, structures)
