from __future__ import annotations

import datetime as dt
import json
import sqlite3

import pytest

from research_factory.pif_discourse_aggregates import collect
from research_factory.signal_desk_intelligence import build_payloads
from research_factory.signal_desk_rebuild_benchmark import (
    BENCHMARK_SOURCE_NAMESPACE,
    BenchmarkIsolationError,
    benchmark_manifest,
    episode_side_token_containment,
    initialize_benchmark_store,
    put_benchmark_transcript,
    transcript_page_matches_episode,
    unwrap_tracking_url,
)


CANONICAL_SCHEMA = """
CREATE TABLE episodes (
  id TEXT PRIMARY KEY, source_id TEXT NOT NULL, title TEXT NOT NULL,
  published_at TEXT, url TEXT, audio_url TEXT
);
CREATE TABLE segments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, text_path TEXT);
CREATE TABLE labels (segment_id TEXT NOT NULL, output_json TEXT NOT NULL);
CREATE TABLE actor_positions (
  id TEXT PRIMARY KEY, segment_id TEXT NOT NULL, actor_name TEXT,
  actor_type TEXT, concept_name TEXT, stance TEXT, claim_type TEXT,
  confidence REAL, evidence_json TEXT NOT NULL
);
CREATE TABLE canonical_people (id TEXT PRIMARY KEY, display_name TEXT);
CREATE TABLE expert_authority_scores (
  canonical_person_id TEXT, score REAL, status TEXT
);
"""


def test_benchmark_store_refuses_canonical_path_or_data_directory(tmp_path):
    canonical = tmp_path / "data" / "factory.sqlite"
    canonical.parent.mkdir()
    with pytest.raises(BenchmarkIsolationError):
        initialize_benchmark_store(canonical, canonical)
    with pytest.raises(BenchmarkIsolationError):
        initialize_benchmark_store(
            canonical.parent / "benchmark.sqlite", canonical
        )


def test_tracking_wrappers_unwrap_without_network_requests():
    target = "https://cdn.example.org/show/feed.xml?x=1"
    assert unwrap_tracking_url(
        "https://chrt.fm/track/ABC123/" + target
    ) == target
    assert unwrap_tracking_url(
        "https://pdst.fm/e/https%3A%2F%2Fcdn.example.org%2Fepisode.mp3"
    ) == "https://cdn.example.org/episode.mp3"
    unknown = "https://example.org/redirect?url=https://unsafe.example/feed"
    assert unwrap_tracking_url(unknown) == unknown


def test_episode_side_containment_does_not_penalize_page_chrome():
    episode = "Why the AI Boom Is Reshaping American Jobs"
    page = (
        "Fresh Air archive and episode transcript: Why the AI Boom Is "
        "Reshaping American Jobs | listen subscribe donate NPR"
    )
    assert episode_side_token_containment(episode, page) == 1.0
    assert transcript_page_matches_episode(episode, page)
    assert not transcript_page_matches_episode(
        episode, "A completely different interview about music"
    )


def test_format_stress_ood_cohort_is_preserved(tmp_path):
    canonical_path = tmp_path / "canonical" / "factory.sqlite"
    canonical_path.parent.mkdir()
    canonical_path.touch()
    benchmark = initialize_benchmark_store(
        tmp_path / "private-benchmark" / "fixtures.sqlite", canonical_path
    )
    put_benchmark_transcript(
        benchmark,
        show_id="ood_gastropod",
        show_name="Gastropod",
        cohort="format_stress",
        episode_id="ood_gastropod_1",
        title="A format-stress fixture",
        official_url="https://example.org/transcript",
        transcript_text="Host: A sufficiently substantial private fixture transcript.",
    )
    assert benchmark_manifest(benchmark)["episodes"][0]["cohort"] == "format_stress"
    benchmark.close()


def test_ood_sentinel_is_physically_and_logically_invisible_to_signal_desk(
    tmp_path,
):
    canonical_path = tmp_path / "canonical" / "factory.sqlite"
    canonical_path.parent.mkdir()
    canonical = sqlite3.connect(canonical_path)
    canonical.row_factory = sqlite3.Row
    canonical.executescript(CANONICAL_SCHEMA)
    canonical.execute(
        "INSERT INTO episodes VALUES (?, ?, ?, ?, ?, ?)",
        ("prod_ep", "production_show", "Production episode", "2026-08-01",
         "https://example.org/prod", None),
    )
    canonical.execute(
        "INSERT INTO segments VALUES (?, ?, ?)",
        ("prod_seg", "prod_ep", None),
    )
    canonical.execute(
        "INSERT INTO labels VALUES (?, ?)",
        ("prod_seg", json.dumps({"topics": [{
            "topic": "production issue", "stance": "neutral", "intensity": 1,
            "evidence": "This is accepted production-only evidence for the issue.",
        }]})),
    )
    canonical.commit()

    benchmark_path = tmp_path / "private-benchmark" / "fixtures.sqlite"
    benchmark = initialize_benchmark_store(benchmark_path, canonical_path)
    sentinel = "OOD_SENTINEL_MUST_NEVER_REACH_SIGNAL_DESK"
    put_benchmark_transcript(
        benchmark,
        show_id="ood_fresh_air",
        show_name=sentinel,
        cohort="claim_dense",
        episode_id="ood_ep_1",
        title="OOD fixture",
        official_url="https://example.org/ood-transcript",
        transcript_text=f"Private fixture text containing {sentinel}.",
    )

    # Substantive boundary check: the real aggregate and presentation builder
    # run over the canonical connection while an OOD fixture exists beside it.
    # SQLite confirms no database was attached, then the complete serialized
    # public payload is searched for both the sentinel and benchmark namespace.
    databases = [row[1] for row in canonical.execute("PRAGMA database_list")]
    assert databases == ["main"]
    aggregate = collect(canonical, now=dt.date(2026, 8, 31))
    public_payload = build_payloads(aggregate)
    public_json = json.dumps(public_payload, sort_keys=True)
    assert sentinel not in public_json
    assert BENCHMARK_SOURCE_NAMESPACE not in public_json
    assert aggregate["corpus"]["shows"] == 1
    assert aggregate["corpus"]["episodes"] == 1

    inventory = benchmark_manifest(benchmark)
    assert inventory["episodes"][0]["display_name"] == sentinel
    assert "transcript_text" not in inventory["episodes"][0]
    canonical.close()
    benchmark.close()
