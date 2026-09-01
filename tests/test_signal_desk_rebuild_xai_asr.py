from __future__ import annotations

import sqlite3

import pytest

from research_factory.signal_desk_rebuild_xai_asr import (
    MIN_DURATION_SECONDS,
    XaiAsrError,
    _content_verified_duration_overage,
    asr_contract,
    select_exact_asr_plan,
    select_asr_plan,
)


def test_frozen_asr_contract_is_explicit_and_hash_stable() -> None:
    contract = asr_contract()
    assert contract["contract_sha256"] == (
        "92893c6a67d487a57b6fa2d322be377d94f4993f391f4a40920914aaec8a10f3"
    )
    assert contract["provider_model"] == "xai_speech_to_text_endpoint_unversioned"
    assert contract["request"] == {
        "input": "url",
        "format": True,
        "language": "en",
        "diarize": True,
        "filler_words": False,
        "vad_threshold": 0.5,
        "keyterms": [],
    }
    assert contract["response"]["timestamp_granularity"] == "word"


def test_exact_replacement_plan_is_bounded_to_six_per_show() -> None:
    conn = _database()
    plan = select_exact_asr_plan(
        conn, [f"search-engine-{index}" for index in range(6)]
    )
    assert plan["replacement_acquisition"] is True
    assert len(plan["shows"]["search-engine"]) == 6
    with pytest.raises(XaiAsrError, match="capped at six"):
        select_exact_asr_plan(
            conn, [f"search-engine-{index}" for index in range(7)]
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
