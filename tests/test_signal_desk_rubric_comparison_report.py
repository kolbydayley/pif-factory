import json
import pytest
from scripts import pif_signal_desk_rubric_comparison_report as report


def test_incomplete_reference_cannot_write_complete_report(tmp_path, monkeypatch):
    monkeypatch.setattr(report, "QUAL", tmp_path)
    monkeypatch.setattr(report, "OUT", tmp_path)
    monkeypatch.setattr(report, "R", tmp_path)
    (tmp_path / "plan.json").write_text(json.dumps({"window_ids": ["w"]}))
    (tmp_path / "merged-manifest.json").write_text(json.dumps({"windows": []}))
    monkeypatch.setattr(report, "prepare", lambda: [{"packet_sha256": "missing"}])
    with pytest.raises(FileNotFoundError):
        report.main()
    assert not (tmp_path / "role-comparison.json").exists()


def test_invalid_reference_cannot_write_complete_report(tmp_path, monkeypatch):
    monkeypatch.setattr(report, "QUAL", tmp_path)
    monkeypatch.setattr(report, "OUT", tmp_path)
    monkeypatch.setattr(report, "R", tmp_path)
    (tmp_path / "plan.json").write_text(json.dumps({"window_ids": ["w"]}))
    (tmp_path / "merged-manifest.json").write_text(json.dumps({"windows": []}))
    (tmp_path / "bad.reference.json").write_text("{}")
    monkeypatch.setattr(report, "prepare", lambda: [{"packet_sha256": "bad"}])
    with pytest.raises(ValueError, match="model/envelope"):
        report.main()
    assert not (tmp_path / "role-comparison.json").exists()
