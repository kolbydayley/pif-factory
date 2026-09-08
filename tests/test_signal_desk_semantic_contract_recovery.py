from copy import deepcopy
import pytest
from scripts.pif_signal_desk_semantic_contract_recovery import partition, repair_packets, reconcile
from research_factory.signal_desk_rubric_reference_packets import digest


def fixture():
    p = {"cases": [{"case_id": "c1"}, {"case_id": "c2"}], "transcript_window": "Actual source.",
        "window_id": "dev", "system_sha256": "old"}
    p["packet_sha256"] = digest(p)
    raw = {"decisions": [{"case_id": cid, "verdict": "supported", "rationale": "Diagnostic only.",
        "proposed_rule_change": "", "source_quotes": [quote]} for cid, quote in [("c1", "Actual source."), ("c2", "Invented.")]]}
    return p, raw


def test_failed_only_full_source_and_unchanged_valid_row():
    p, raw = fixture(); parts, packets = repair_packets(p, raw, lambda s: 1)
    assert len(parts["retained"]) == len(packets) == 1
    assert packets[0]["transcript_window"] == p["transcript_window"]
    repaired = {"decisions": [dict(raw["decisions"][1], source_quotes=["Actual source."])]}
    value, receipt = reconcile(p, raw, [(packets[0], repaired)])
    assert value["decisions"][0] == raw["decisions"][0]
    assert receipt["first_pass_failure_preserved"] and not receipt["gold_accepted"]


def test_duplicate_invalid_and_extras_preserved():
    p, raw = fixture()
    raw["decisions"].append(deepcopy(raw["decisions"][0]))
    raw["decisions"].append(dict(raw["decisions"][0], case_id="extra"))
    parts = partition(p, raw)
    assert len(parts["failures"]) == 2 and len(parts["extras"]) == 1


def test_no_source_truncation_to_fit():
    p, raw = fixture()
    with pytest.raises(ValueError): repair_packets(p, raw, lambda s: 12000)


@pytest.mark.parametrize("mutation", ["source", "candidate", "missing", "duplicate"])
def test_reconciliation_lineage_and_population(mutation):
    p, raw = fixture(); _, packets = repair_packets(p, raw, lambda s: 1)
    r = packets[0]; response = {"decisions": [dict(raw["decisions"][1], source_quotes=["Actual source."])]}
    pairs = [(r, response)]
    if mutation == "source": r["transcript_window"] = "Changed."
    elif mutation == "candidate": r["cases"][0]["injected"] = True
    elif mutation == "missing": pairs = []
    else: pairs *= 2
    with pytest.raises(ValueError): reconcile(p, raw, pairs)
