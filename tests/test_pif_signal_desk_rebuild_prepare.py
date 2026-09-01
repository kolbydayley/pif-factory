from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts/pif_signal_desk_rebuild_prepare.py"
SPEC = importlib.util.spec_from_file_location("pif_signal_desk_rebuild_prepare", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_private_packets_require_a_frozen_local_artifact(monkeypatch, tmp_path):
    monkeypatch.setattr(MODULE, "build_split_manifest", lambda *a, **k: {
        "manifest_sha256": "a" * 64,
        "coverage_diagnostics": [],
        "counts": {"windows": 12},
    })
    monkeypatch.setattr(MODULE, "freeze_per_show_artifacts", lambda manifest: ({
        "show": {"show_id": "show-a"},
    },))
    database = tmp_path / "factory.sqlite"
    import sqlite3
    sqlite3.connect(database).close()
    aliases = tmp_path / "aliases.json"
    aliases.write_text('{"aliases": {}}')
    with pytest.raises(ValueError, match="requires --freeze-qualified"):
        MODULE.prepare(
            database=database,
            project_root=tmp_path,
            output_root=tmp_path / "out",
            show_alias_path=aliases,
            freeze=False,
            materialize_gold_packets=True,
        )
