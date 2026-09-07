import json
import pytest
from research_factory import signal_desk_attribution_contrasts as module


def args(tmp_path, monkeypatch):
    ids = [str(i) for i in range(16)]
    monkeypatch.setattr(module, "_load_frozen_window_text", lambda *a, **k: "full untouched source")
    monkeypatch.setattr(module, "validate_output", lambda *a, **k: None)
    (tmp_path / "C").mkdir()
    for wid in ids:
        events = [{"event_id": str(i), "claim_text": "claim", "evidence_text": "source", "evidence_start": 15,
            "evidence_end": 21, "speaker_id": "DO NOT LEAK", "stance": "supportive"} for i in range(5)] if wid != "0" else []
        (tmp_path / "C" / f"{wid}.json").write_text(json.dumps({"events": events}))
    return dict(qualification_plan={"window_ids": ids, "manifest_sha256": "m"},
        manifest={"manifest_sha256": "m", "windows": [{"window_id": wid, "show_id": wid,
            "split": "development", "text_sha256": "t", "transcript_structure": "flattened"} for wid in ids]},
        result_root=tmp_path, project_root=tmp_path)


def test_keeps_full_source_empty_window_and_hides_old_labels(tmp_path, monkeypatch):
    out = module.build(**args(tmp_path, monkeypatch))
    assert len(out["packets"]) == 16
    assert out["packets"][0]["empty_anchor_window"]
    assert all(p["transcript_window"] == "full untouched source" for p in out["packets"])
    assert len(out["packets"][1]["anchors"]) == 3
    assert all(set(a) == set(module.ANCHOR_FIELDS) for p in out["packets"] for a in p["anchors"])
    assert not out["recall_gate_eligible"] and not out["representative_corpus_estimate"]


def test_sealed_source_rejected_before_read(tmp_path, monkeypatch):
    kw = args(tmp_path, monkeypatch); kw["manifest"]["windows"][0]["split"] = "sealed_holdout"
    with pytest.raises(ValueError, match="protected"):
        module.build(**kw)


def test_shrunk_population_rejected(tmp_path, monkeypatch):
    kw = args(tmp_path, monkeypatch); kw["qualification_plan"]["window_ids"].pop()
    with pytest.raises(ValueError, match="16-window"):
        module.build(**kw)
