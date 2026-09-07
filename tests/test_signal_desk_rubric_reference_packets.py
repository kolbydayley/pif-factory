import pytest
from research_factory.signal_desk_rubric_reference_packets import build_reference_packets
from research_factory.signal_desk_gold_shared_rubric import receipt
from research_factory.signal_desk_rubric_reference_packets import validate_reference
from research_factory.signal_desk_rubric_reference_packets import bounded_groups


def test_lossless_batches_respect_both_limits():
    events=list(range(60))
    groups=bounded_groups(events,max_events=25,fits=lambda g:len(g)<=17)
    assert [e for g in groups for e in g]==events
    assert [len(g) for g in groups]==[17,17,17,9]


def test_candidate_cap_is_hard_even_with_unlimited_tokens():
    assert [len(g) for g in bounded_groups(list(range(51)),max_events=25,fits=lambda g:True)]==[25,25,1]


def test_oversized_event_is_not_dropped_or_truncated():
    with pytest.raises(ValueError,match="no truncation"):
        bounded_groups(["small","oversized"],max_events=25,fits=lambda g:"oversized" not in g)


def test_empty_window_still_gets_one_packet():
    assert bounded_groups([],max_events=25,fits=lambda g:True)==[[]]


def reference():
    return {"model":"gpt-5.5","empty_window_verdict":"not_applicable","empty_window_rationale":"",
        "decisions":[{"event_id":"e","verdict":"supported","source_basis":"explicit labeled assertion",
            "correction_json":"{}","missing_context":"","strategic_relevance":"incidental","relevance_rationale":"show housekeeping, not industry change"}]}


def test_supported_but_incidental_is_not_promoted_to_strategic():
    out=validate_reference(reference(),{"candidate_events":[{"event_id":"e"}]})
    assert out["decisions"][0]["strategic_relevance"]=="incidental"


def test_supported_cannot_hide_a_patch():
    out=reference();out["decisions"][0]["correction_json"]='{"speaker_id":"guessed"}'
    with pytest.raises(ValueError,match="silently change"):validate_reference(out,{"candidate_events":[{"event_id":"e"}]})


def test_empty_candidates_need_a_real_judgment():
    out=reference();out["decisions"]=[]
    with pytest.raises(ValueError,match="empty window requires"):validate_reference(out,{"candidate_events":[]})


def test_missing_event_cannot_vanish_from_denominator():
    with pytest.raises(ValueError,match="incomplete"):validate_reference(reference(),{"candidate_events":[{"event_id":"e"},{"event_id":"missing"}]})


def args(tmp_path):
    ids=[str(i) for i in range(16)]
    return dict(plan={"rubric":receipt(),"manifest_sha256":"m","window_ids":ids},
        manifest={"manifest_sha256":"m","windows":[{"window_id":i,"split":"development"} for i in ids]},
        result_root=tmp_path,project_root=tmp_path,token_count=len)


def test_rejects_benchmark_shrink_before_reading_sources(tmp_path):
    kw=args(tmp_path);kw["plan"]["window_ids"].pop()
    with pytest.raises(ValueError,match="complete frozen 16"):build_reference_packets(**kw)


def test_rejects_sealed_item_before_reading_sources(tmp_path):
    kw=args(tmp_path);kw["manifest"]["windows"][0]["split"]="sealed_holdout"
    with pytest.raises(ValueError,match="non-development"):build_reference_packets(**kw)


def test_rejects_changed_rubric(tmp_path):
    kw=args(tmp_path);kw["plan"]["rubric"]["sha256"]="stale"
    with pytest.raises(ValueError,match="rubric changed"):build_reference_packets(**kw)


def test_missing_output_stops_inventory_before_any_packets(tmp_path,monkeypatch):
    from research_factory import signal_desk_rubric_reference_packets as module
    monkeypatch.setattr(module,"_load_frozen_window_text",lambda *a,**k:"frozen")
    with pytest.raises(ValueError,match="missing A"):build_reference_packets(**args(tmp_path))
