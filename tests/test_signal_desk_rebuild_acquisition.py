from __future__ import annotations

import sqlite3

import pytest

from research_factory.signal_desk_rebuild_acquisition import (
    CHANNEL_SANITY_MIN_MATCH,
    EPISODES_PER_SHOW,
    channel_sanity,
    plan_blocked_show_acquisition,
    select_period_spread,
    validate_transcript_ingest,
)


SHOWS = (
    "how-i-built-this",
    "marketplace-tech",
    "search-engine",
    "tech-brew-ride-home",
    "the-ben-and-marc-show",
)


def database() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE episodes (
            id TEXT PRIMARY KEY, source_id TEXT, title TEXT, published_at TEXT,
            duration_seconds INTEGER, audio_url TEXT, url TEXT,
            verified_transcript_url TEXT
        )"""
    )
    for show in SHOWS:
        for index in range(40):
            transcript = (
                f"https://www.npr.org/transcripts/{1000 + index}"
                if show == "how-i-built-this" else None
            )
            conn.execute(
                "INSERT INTO episodes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    f"{show}-{index:02d}", show,
                    f"companytoken{index} executiveword{index} subjectword{index}",
                    f"20{20 + index // 12:02d}-{index % 12 + 1:02d}-01",
                    3600, f"https://audio.test/{show}/{index}.mp3",
                    f"https://example.test/{show}/{index}", transcript,
                ),
            )
    return conn


def passing_listing() -> list[dict[str, object]]:
    return [
        {"id": f"v{index}",
         "title": f"companytoken{index} executiveword{index} subjectword{index}",
         "duration": 3600}
        for index in range(40)
    ]


def test_period_spread_is_episode_disjoint_and_reaches_catalog_extremes() -> None:
    conn = database()
    rows = conn.execute("SELECT * FROM episodes WHERE source_id=? ORDER BY published_at, id",
                        ("search-engine",)).fetchall()
    selected = select_period_spread(rows)
    assert len(selected) == EPISODES_PER_SHOW
    assert len({row["id"] for row in selected}) == EPISODES_PER_SHOW
    assert selected[0]["id"] == rows[0]["id"]
    assert selected[-1]["id"] == rows[-1]["id"]


def test_plan_is_bounded_and_missing_key_names_only_unavoidable_asr_blockers() -> None:
    plan = plan_blocked_show_acquisition(database(), youtube_listing=passing_listing(),
                                         groq_api_key="")
    assert plan["full_catalog_asr_allowed"] is False
    assert plan["ready_for_all_five"] is False
    assert set(plan["shows"]) == set(SHOWS)
    assert all(len(show["candidates"]) == 4 for show in plan["shows"].values())
    blocker = plan["prerequisites"][0]
    assert blocker["code"] == "missing_groq_api_key"
    assert blocker["blocks"] == ["search-engine", "tech-brew-ride-home"]
    assert blocker["conditionally_blocks_fallback_for"] == [
        "how-i-built-this", "marketplace-tech", "the-ben-and-marc-show",
    ]
    assert "full-catalog ASR is out of scope" in blocker["message"]


def test_hibt_is_browser_only_marketplace_browser_then_asr_and_ben_caption_first() -> None:
    plan = plan_blocked_show_acquisition(database(), youtube_listing=passing_listing(),
                                         groq_api_key="test-key")
    assert plan["asr_credential_available"] is True
    assert plan["ready_for_all_five"] is False
    hibt = plan["shows"]["how-i-built-this"]
    assert {row["primary_lane"] for row in hibt["candidates"]} == {"browser_npr"}
    assert all(row["fallback_lane"] == "groq_asr" for row in hibt["candidates"])
    marketplace = plan["shows"]["marketplace-tech"]
    assert all(row["primary_lane"] == "browser_marketplace" and
               row["fallback_lane"] == "groq_asr" for row in marketplace["candidates"])
    assert all(row["transcript_url"].startswith("https://example.test/")
               for row in marketplace["candidates"])
    ben = plan["shows"]["the-ben-and-marc-show"]
    assert ben["channel_sanity"]["passed"] is True
    assert all(row["primary_lane"] == "youtube_caption_browser" for row in ben["candidates"])
    assert all(row["duration_seconds"] >= 1500 for row in ben["candidates"])


def test_channel_gate_requires_full_40_episode_sample_and_25_percent_match() -> None:
    conn = database()
    rows = conn.execute(
        "SELECT * FROM episodes WHERE source_id=? ORDER BY published_at, id",
        ("the-ben-and-marc-show",),
    ).fetchall()
    ten_matches = passing_listing()[:10] + [
        {"id": f"x{n}", "title": f"unrelated material {n}", "duration": 3600}
        for n in range(30)
    ]
    result = channel_sanity(rows, ten_matches)
    assert result["sample_size"] == 40
    assert result["score"] == CHANNEL_SANITY_MIN_MATCH
    assert result["passed"] is True
    assert channel_sanity(rows[:39], passing_listing())["passed"] is False


def test_channel_gate_excludes_short_or_unknown_duration_uploads() -> None:
    conn = database()
    rows = conn.execute(
        "SELECT * FROM episodes WHERE source_id=? ORDER BY published_at, id",
        ("the-ben-and-marc-show",),
    ).fetchall()
    listing = passing_listing()
    for video in listing:
        video["duration"] = 1499 if int(str(video["id"])[1:]) < 20 else None
    assert channel_sanity(rows, listing)["passed"] is False


def test_ingest_guard_is_words_at_least_duration_divided_by_ten_minutes() -> None:
    # 3600 seconds requires 600 words (10 words/minute).
    with pytest.raises(ValueError, match="client-rendered shell suspected"):
        validate_transcript_ingest(text="word " * 599, duration_seconds=3600,
                                   episode_id="shell")
    validate_transcript_ingest(text="word " * 600, duration_seconds=3600,
                               episode_id="real")
