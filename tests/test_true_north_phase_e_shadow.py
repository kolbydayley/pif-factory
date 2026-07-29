from __future__ import annotations

from pathlib import Path

import pytest

from research_factory import true_north_phase_e_shadow as phase_e


def test_dataless_database_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    database = tmp_path / "factory.sqlite"
    database.touch()
    monkeypatch.setattr(
        phase_e,
        "database_identity",
        lambda _path: {
            "path": str(database),
            "size_bytes": 10,
            "inode": 1,
            "mtime_ns": 1,
            "flags": phase_e.SF_DATALESS,
            "dataless": True,
        },
    )

    result = phase_e.strict_isolation_preflight(
        production_database=database,
        shadow_root=tmp_path / "isolated-shadow",
    )

    assert result["eligible_to_dispatch"] is False
    assert result["provider_calls_made"] == 0
    assert result["stop_reasons"] == [
        "production_database_is_dataless_and_unreadable"
    ]
    with pytest.raises(phase_e.PhaseEShadowError):
        phase_e.require_dispatch_eligibility(result)


def test_shadow_store_must_not_overlap_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "factory.sqlite"
    database.touch()

    result = phase_e.strict_isolation_preflight(
        production_database=database,
        shadow_root=database,
    )

    assert result["eligible_to_dispatch"] is False
    assert "shadow_store_overlaps_production_database" in result[
        "stop_reasons"
    ]
