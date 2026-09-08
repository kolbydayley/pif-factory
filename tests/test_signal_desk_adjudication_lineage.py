from copy import deepcopy
import pytest
from research_factory import signal_desk_adjudication_lineage as lineage
from test_signal_desk_full_event_v5 import value, SOURCE


def envelope():
    output=value(); eid=output["events"][0]["event_id"]
    return {"schema_version":lineage.VERSION,"records":output,"additions":[],
            "input_dispositions":[{"author":a,"input_event_id":eid,"action":"merged",
             "output_event_ids":[eid],"reason":"Equivalent propositions independently extracted."} for a in ("A","B")]}


def check(v, a=None, b=None):
    return lineage.validate(v,source=SOURCE,window_id="dev",author_a=value() if a is None else a,author_b=value() if b is None else b)


def test_same_id_in_two_authors_maps_to_one_output_without_losing_denominator():
    v=check(envelope())
    assert len(v["input_dispositions"])==2 and len(v["records"]["events"])==1
    assert not lineage.receipt()["qualified"]


@pytest.mark.parametrize("failure",["missing","duplicate","unknown_input","unknown_output","duplicate_ref","split_one","rejected_ref","blank_reason","unresolved_candidate","addition_overlap","missing_output_lineage"])
def test_fail_closed(failure):
    v=envelope(); row=v["input_dispositions"][0]
    if failure=="missing":v["input_dispositions"].pop()
    elif failure=="duplicate":v["input_dispositions"].append(deepcopy(row))
    elif failure=="unknown_input":row["input_event_id"]="other"
    elif failure=="unknown_output":row["output_event_ids"]=["other"]
    elif failure=="duplicate_ref":row["output_event_ids"]*=2
    elif failure=="split_one":row["action"]="split"
    elif failure=="rejected_ref":row["action"]="rejected"
    elif failure=="blank_reason":row["reason"]="  "
    elif failure=="unresolved_candidate":row["action"]="unresolved"
    elif failure=="addition_overlap":v["additions"]=[{"output_event_id":row["output_event_ids"][0],"reason":"New"}]
    else:
        for r in v["input_dispositions"]:r.update(action="rejected",output_event_ids=[])
    with pytest.raises(ValueError):check(v)


def test_rejected_inputs_remain_in_ledger_even_when_no_records():
    v=envelope();v["records"].update(events=[],voice_bindings=[],window_disposition="no_records")
    for r in v["input_dispositions"]:r.update(action="rejected",output_event_ids=[],reason="Source does not support the proposed claim.")
    check(v)


def test_unresolved_is_not_accepted_or_silently_deleted():
    v=envelope();v["records"]["events"][0]["publishability_state"]="uncertain"
    for r in v["input_dispositions"]:r["action"]="unresolved"
    check(v)


def test_source_discovered_addition_requires_explicit_accountability():
    v=envelope();empty=value();empty.update(events=[],voice_bindings=[],window_disposition="no_records")
    v["input_dispositions"]=[]
    with pytest.raises(ValueError,match="unaccounted output"):check(v,empty,empty)
    v["additions"]=[{"output_event_id":v["records"]["events"][0]["event_id"],"reason":"Both authors omitted this source proposition."}]
    check(v,empty,empty)


def test_single_input_cannot_claim_to_be_a_merge():
    v=envelope();empty=value();empty.update(events=[],voice_bindings=[],window_disposition="no_records")
    v["input_dispositions"].pop()
    with pytest.raises(ValueError,match="multiple inputs"):check(v,b=empty)


def test_packet_binds_unchanged_full_source_and_both_parents():
    from research_factory.signal_desk_rubric_reference_packets import digest
    s={"window_id":"dev","transcript_window":SOURCE,"transcript_structure":"speaker_turn"}
    s["packet_sha256"]=digest(s)
    p=lineage.packet(s,author_a=value(),author_b=value())
    assert p["transcript_window"]==SOURCE and p["author_a"]==p["author_b"]==value()
    assert p["system_sha256"]==digest(lineage.system())
    assert p["schema_sha256"]==digest(lineage.schema())
    assert "audit" not in p and lineage.RULES in lineage.system()
    s["transcript_window"]+=" tamper"
    with pytest.raises(ValueError,match="lineage changed"):lineage.packet(s,author_a=value(),author_b=value())


def test_split_can_retain_distinct_propositions_sharing_one_span():
    v=envelope();second=deepcopy(v["records"]["events"][0]);second["event_id"]="second"
    second["claim_text"]="A distinct proposition requires independent source review."
    v["records"]["events"].append(second)
    for row in v["input_dispositions"]:
        row.update(action="split",output_event_ids=[v["records"]["events"][0]["event_id"],"second"])
    # Byte/lineage validity cannot establish that these propositions are distinct.
    # That is explicitly left to independent source review, not hidden heuristics.
    check(v)
