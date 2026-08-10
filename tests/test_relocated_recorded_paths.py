from pathlib import Path

import research_factory.paths as paths


def test_legacy_project_prefix_resolves_under_active_root(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "root", lambda: tmp_path / "pif-factory")
    old = "/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory/runs/outputs/x.json"
    assert paths.resolve_recorded_path(old) == (tmp_path / "pif-factory/runs/outputs/x.json").resolve()


def test_external_absolute_path_is_not_rewritten(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "root", lambda: tmp_path / "pif-factory")
    external = Path("/Users/kolbydayley/pif-backups/restore.sqlite")
    assert paths.resolve_recorded_path(external) == external
