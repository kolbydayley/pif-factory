import json
import pytest
from research_factory.signal_desk_semantic_contract_report import summarize
from research_factory.signal_desk_rubric_reference_packets import digest
from scripts.pif_signal_desk_semantic_contract_review import validate


def setup(root, completed=True):
    family = {"family_id": "test", "qualified": False}
    packet = {"family": family, "window_id": "dev", "transcript_window": "Source.",
        "source_sha256": digest("Source."), "system_sha256": digest("rules"),
        "review_schema_sha256": "schema", "cases": [{"case_id": "c1"}]}
    sha = digest(packet); packet["packet_sha256"] = sha
    plan = {"packets": [sha], "families": [family], "system": "rules",
        "review_schema_sha256": "schema", "sampling": "Test, not a benchmark"}
    write(root / "plan.json", plan); write(root / f"{sha}.packet.json", packet)
    if completed:
        value = {"decisions": [{"case_id": "c1", "verdict": "revise", "rationale": "Needs distinction.",
            "proposed_rule_change": "Separate observed and imagined.", "source_quotes": ["Source."]}]}
        write(root / f"{sha}.review.json", value); write(root / f"{sha}.output.json", value)
        write(root / f"{sha}.sidecar.json", {"state": "completed"})
    return sha


def write(path, value):
    path.write_text(json.dumps(value))


def test_complete_diagnostic_is_not_gold(tmp_path):
    setup(tmp_path)
    report = summarize(tmp_path, validate)
    assert report["complete"] and report["validated_cases"] == report["expected_cases"] == 1
    assert report["verdicts"] == {"revise": 1}
    assert len(report["action_references"]) == 1
    assert not report["gold_accepted"] and not report["gate_eligible"]


def test_incomplete_denominator_preserved(tmp_path):
    setup(tmp_path, completed=False)
    report = summarize(tmp_path, validate)
    assert not report["complete"] and report["expected_cases"] == 1 and report["validated_cases"] == 0


@pytest.mark.parametrize("mutation", ["packet", "sidecar", "raw", "pending", "plan"])
def test_tampering_fails_closed(tmp_path, mutation):
    sha = setup(tmp_path)
    if mutation == "packet":
        path = tmp_path / f"{sha}.packet.json"; value = json.loads(path.read_text())
        value["transcript_window"] = "Changed."; write(path, value)
    elif mutation == "sidecar": write(tmp_path / f"{sha}.sidecar.json", {"state": "in_progress"})
    elif mutation == "raw": write(tmp_path / f"{sha}.output.json", {})
    elif mutation == "pending": write(tmp_path / f"{sha}.pending.json", {})
    else:
        path = tmp_path / "plan.json"; value = json.loads(path.read_text())
        value["packets"].append(sha); write(path, value)
    with pytest.raises(ValueError): summarize(tmp_path, validate)
