from copy import deepcopy
import json
import pytest
from scripts.pif_signal_desk_semantic_contract_recovery import partition, repair_packets, reconcile, collect, validate_packet_inventory, SYSTEM, schema
from research_factory.signal_desk_rubric_reference_packets import digest


def fixture():
    p = {"cases": [{"case_id": "c1"}, {"case_id": "c2"}], "transcript_window": "Actual source.",
        "window_id": "dev", "system_sha256": "old"}
    p["packet_sha256"] = digest(p)
    raw = {"decisions": [{"case_id": cid, "verdict": "supported", "rationale": "Diagnostic only.",
        "proposed_rule_change": "", "source_quotes": [quote]} for cid, quote in [("c1", "Actual source."), ("c2", "Invented.")]]}
    return p, raw


def test_canonical_mapping_order_does_not_change_inventory():
    validate_packet_inventory(["a", "b"], ["b", "a"])


@pytest.mark.parametrize("left,right", [(["a", "a"], ["a"]), (["a"], ["a", "a"]), (["a"], ["b"])])
def test_changed_inventory_still_rejected(left, right):
    with pytest.raises(ValueError): validate_packet_inventory(left, right)


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


@pytest.mark.parametrize("tamper", [False, True])
def test_collect_checks_actual_raw_output(tmp_path, tamper):
    original = tmp_path / "original"; recovery = tmp_path / "recovery"
    original.mkdir(); recovery.mkdir()
    def save(path, value): path.write_text(json.dumps(value))
    p, raw = fixture(); parts, packets = repair_packets(p, raw, lambda s: 1)
    sha = p["packet_sha256"]; r = packets[0]; rsha = r["packet_sha256"]
    op = {"packets": [sha]}
    save(original / "plan.json", op)
    save(original / f"{sha}.packet.json", p); save(original / f"{sha}.output.json", raw)
    save(recovery / "plan.json", {"original_plan_sha256": digest(op), "system": SYSTEM,
        "schema_sha256": digest(schema()), "packets": [rsha],
        "inventory": {sha: {"partition": parts, "packets": [rsha]}}})
    save(recovery / f"{rsha}.packet.json", r)
    value = {"decisions": [dict(raw["decisions"][1], source_quotes=["Actual source."])]}
    save(recovery / f"{rsha}.review.json", value)
    save(recovery / f"{rsha}.output.json", {} if tamper else value)
    save(recovery / f"{rsha}.sidecar.json", {"state": "completed"})
    if tamper:
        with pytest.raises(ValueError): collect(original, recovery)
    else:
        outputs, receipts, pending = collect(original, recovery)
        assert not pending and len(outputs[sha]["decisions"]) == 2
        assert receipts[sha]["partition"]["retained"] == [raw["decisions"][0]]
