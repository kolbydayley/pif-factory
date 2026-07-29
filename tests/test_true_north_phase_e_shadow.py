from __future__ import annotations

from pathlib import Path
import sqlite3

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


def test_shadow_store_must_not_be_production_parent(
    tmp_path: Path,
) -> None:
    database = tmp_path / "factory.sqlite"
    database.touch()

    result = phase_e.strict_isolation_preflight(
        production_database=database,
        shadow_root=tmp_path,
    )

    assert result["eligible_to_dispatch"] is False
    assert "shadow_store_overlaps_production_database" in result[
        "stop_reasons"
    ]


def test_executable_preflight_proves_read_only_and_distinct(
    tmp_path: Path,
) -> None:
    database = tmp_path / "authority" / "factory.sqlite"
    database.parent.mkdir()
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE marker (id INTEGER)")
    connection.commit()
    connection.close()

    result = phase_e.executable_isolation_preflight(
        production_database=database,
        shadow_root=tmp_path / "shadow",
    )

    assert result["eligible_to_dispatch"] is True
    assert result["executable_checks"]["query_only"] == 1
    assert result["executable_checks"]["write_probe_rejected"] is True
    verify = sqlite3.connect(database)
    try:
        tables = {
            row[0]
            for row in verify.execute(
                "SELECT name FROM sqlite_schema WHERE type='table'"
            )
        }
    finally:
        verify.close()
    assert "pif_phase_e_write_probe" not in tables


def test_recent_selection_requires_query_only() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        with pytest.raises(
            phase_e.PhaseEShadowError, match="requires query_only"
        ):
            phase_e.select_recent_episodes(connection)
    finally:
        connection.close()


def test_recent_selection_requires_ready_transcript_and_codex_baseline() -> None:
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE episodes (
          id TEXT PRIMARY KEY,
          source_id TEXT,
          title TEXT,
          published_at TEXT,
          created_at TEXT,
          updated_at TEXT
        );
        CREATE TABLE transcripts (
          id TEXT PRIMARY KEY,
          episode_id TEXT,
          status TEXT,
          updated_at TEXT
        );
        CREATE TABLE segments (
          id TEXT PRIMARY KEY,
          episode_id TEXT
        );
        CREATE TABLE labels (
          id TEXT PRIMARY KEY,
          segment_id TEXT,
          status TEXT,
          model TEXT
        );
        """
    )
    for index in range(4):
        episode_id = f"ep{index}"
        connection.execute(
            "INSERT INTO episodes VALUES (?, 'src', ?, ?, ?, ?)",
            (
                episode_id,
                f"Episode {index}",
                f"2026-07-{20 + index:02d}",
                "2026-07-01",
                "2026-07-01",
            ),
        )
        connection.execute(
            "INSERT INTO transcripts VALUES (?, ?, 'ready', '2026-07-25')",
            (f"tr{index}", episode_id),
        )
        connection.execute(
            "INSERT INTO segments VALUES (?, ?)",
            (f"seg{index}", episode_id),
        )
        connection.execute(
            "INSERT INTO labels VALUES (?, ?, 'accepted', 'gpt-5.5')",
            (f"lab{index}", f"seg{index}"),
        )
    connection.execute("PRAGMA query_only = ON")
    try:
        selected = phase_e.select_recent_episodes(connection, limit=3)
    finally:
        connection.close()

    assert [row["episode_id"] for row in selected] == [
        "ep3",
        "ep2",
        "ep1",
    ]


def test_measurement_plan_has_closed_budget() -> None:
    plan = phase_e.measurement_plan()

    assert plan["declared_budget"] == {
        "max_calls": 60,
        "max_tokens": 1_500_000,
        "status": "recorded_not_opened",
    }
    assert plan["isolation"]["queue_mutation"] is False


def test_certified_hybrid_budget_preflight_fails_before_dispatch(
    tmp_path: Path,
) -> None:
    database = tmp_path / "production.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL)"
    )
    for episode_index, count in enumerate((19, 13, 16), start=1):
        connection.executemany(
            "INSERT INTO segments (id, episode_id) VALUES (?, ?)",
            [
                (f"seg_{episode_index}_{index}", f"ep_{episode_index}")
                for index in range(count)
            ],
        )
    connection.commit()
    connection.close()

    with phase_e.open_read_only_database(database) as read_only:
        result = phase_e.certified_hybrid_budget_preflight(
            read_only,
            episode_ids=["ep_1", "ep_2", "ep_3"],
        )

    assert result["segment_count"] == 48
    assert result["minimum_provider_calls_before_compound_sol_lanes"] == 144
    assert result["eligible_to_dispatch"] is False
    assert result["provider_calls_made"] == 0
    with pytest.raises(
        phase_e.PhaseEShadowError,
        match="certified_exact_contract_exceeds_declared_call_ceiling",
    ):
        phase_e.require_hybrid_budget_eligibility(result)


def test_one_episode_budget_preflight_counts_complete_certified_lanes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "production.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE segments (
            id TEXT PRIMARY KEY,
            episode_id TEXT NOT NULL,
            segment_index INTEGER NOT NULL
        );
        CREATE TABLE discourse_events (
            id TEXT PRIMARY KEY,
            segment_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            claim_text TEXT
        );
        """
    )
    for index in range(16):
        connection.execute(
            "INSERT INTO segments VALUES (?, 'episode', ?)",
            (f"seg_{index}", index),
        )
        if index < 14:
            connection.execute(
                "INSERT INTO discourse_events VALUES (?, ?, 0, ?)",
                (f"candidate_{index}", f"seg_{index}", "A and B"),
            )
    connection.commit()
    connection.close()

    with phase_e.open_read_only_database(database) as read_only:
        result = phase_e.certified_hybrid_episode_budget_preflight(
            read_only,
            episode_id="episode",
        )

    assert result["stored_segment_count"] == 16
    assert result["candidate_segment_count"] == 14
    assert result["compound_segment_count"] == 14
    assert result["exact_contract_optimistic_call_floor"] == {
        "disposition_glm_pass_a": 14,
        "disposition_glm_pass_b": 14,
        "task5_glm_decomposition": 14,
        "compound_sol_pass_b_envelopes": 7,
        "compound_sol_c2_adjudication": 14,
    }
    assert result["minimum_provider_calls"] == 63
    assert result["eligible_to_dispatch"] is False
