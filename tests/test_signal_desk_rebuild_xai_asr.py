from __future__ import annotations

import sqlite3

import pytest

from research_factory.signal_desk_rebuild_xai_asr import (
    MIN_DURATION_SECONDS,
    XaiAsrError,
    _content_verified_duration_overage,
    select_asr_plan,
)


def _database() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE episodes (
             id TEXT PRIMARY KEY, source_id TEXT, title TEXT, published_at TEXT,
             duration_seconds INTEGER, audio_url TEXT
           )"""
    )
    for source_id in ("how-i-built-this", "search-engine"):
        conn.execute(
            "INSERT INTO episodes VALUES (?,?,?,?,?,?)",
            (f"{source_id}-promo", source_id, "Promo", "2020-01-01", 120, "https://audio/promo"),
        )
        for index in range(8):
            conn.execute(
                "INSERT INTO episodes VALUES (?,?,?,?,?,?)",
                (
                    f"{source_id}-{index}",
                    source_id,
                    f"Episode {index}",
                    f"202{index}-01-01",
                    MIN_DURATION_SECONDS + index,
                    f"https://audio/{source_id}/{index}.mp3",
                ),
            )
    return conn


def test_plan_is_four_period_spread_episodes_and_excludes_promos() -> None:
    plan = select_asr_plan(_database(), ("how-i-built-this", "search-engine"))
    assert plan["episodes_per_show"] == 4
    assert plan["full_catalog_asr_allowed"] is False
    for rows in plan["shows"].values():
        assert len(rows) == 4
        assert len({row["episode_id"] for row in rows}) == 4
        assert all(row["duration_seconds"] >= MIN_DURATION_SECONDS for row in rows)
        assert not any(row["episode_id"].endswith("promo") for row in rows)


def test_plan_rejects_unbounded_or_unknown_show() -> None:
    with pytest.raises(XaiAsrError, match="outside the bounded"):
        select_asr_plan(_database(), ("unapproved-show",))


def test_duration_overage_requires_bounded_delta_and_two_title_tokens() -> None:
    row = {
        "title": "Spanx: Sara Blakely",
        "duration_seconds": 1561,
    }
    assert _content_verified_duration_overage(
        row,
        text="Sara Blakely explains the company and its early years.",
        observed_duration=1970,
    )
    assert not _content_verified_duration_overage(
        row,
        text="A different guest explains a different company.",
        observed_duration=1970,
    )
    assert not _content_verified_duration_overage(
        row,
        text="Sara Blakely explains the company and its early years.",
        observed_duration=2200,
    )
