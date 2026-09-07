import pytest
from research_factory.signal_desk_rubric_reference_packets import build_reference_packets
from research_factory.signal_desk_gold_shared_rubric import receipt


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
