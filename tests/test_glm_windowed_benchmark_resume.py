from pathlib import Path

import pytest

from research_factory.glm_windowed_benchmark_resume import _require_shadow


def test_require_shadow_accepts_scoring_child(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import research_factory.glm_windowed_benchmark_resume as module

    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    expected = (tmp_path / "shadow" / "scoring" / "run" / "file.json").resolve()
    assert _require_shadow(expected) == expected


def test_require_shadow_rejects_outside_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import research_factory.glm_windowed_benchmark_resume as module

    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    with pytest.raises(Exception):
        _require_shadow(tmp_path / "elsewhere" / "file.json")
