import json
import sqlite3
from pathlib import Path

import pytest

from research_factory.signal_desk_rebuild_gold import (
    GLM_CONTRACT_VERSION,
    SignalDeskGoldError,
    build_gold_packets,
    build_split_manifest,
    select_gold_audit,
    verify_frozen_manifest,
)


def _database(root: Path, *, duplicate_feed: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE sources (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, rss_url TEXT, enabled INTEGER
        );
        CREATE TABLE episodes (
          id TEXT PRIMARY KEY, source_id TEXT, guid TEXT, title TEXT, published_at TEXT
        );
        CREATE TABLE transcripts (
          id TEXT PRIMARY KEY, episode_id TEXT, raw_text_path TEXT,
          raw_text_sha256 TEXT, status TEXT
        );
        CREATE TABLE transcript_preparations (
          id TEXT PRIMARY KEY, transcript_id TEXT, status TEXT,
          cleaned_text_path TEXT, cleaned_text_sha256 TEXT, quality_score REAL
        );
        """
    )
    sources = [("show-a", "Show A", "https://feeds.example/a?utm_source=x", 1)]
    if duplicate_feed:
        sources.append(("show-alias", "Show Alias", "https://feeds.example/a", 1))
    conn.executemany("INSERT INTO sources VALUES (?,?,?,?)", sources)
    for source_id, *_ in sources:
        for index in range(5):
            episode_id = f"{source_id}-ep-{index}"
            text = "\n\n".join(
                f"Speaker {turn % 2 + 1}: Episode {index} turn {turn}. "
                f"This is a consequential statement with unique marker {source_id}-{index}-{turn}."
                for turn in range(90)
            )
            path = root / f"{episode_id}.txt"
            path.write_text(text, encoding="utf-8")
            digest = __import__("hashlib").sha256(text.encode()).hexdigest()
            conn.execute(
                "INSERT INTO episodes VALUES (?,?,?,?,?)",
                (episode_id, source_id, f"guid-{index}", f"Episode {index}", f"2026-08-{index + 1:02d}"),
            )
            conn.execute(
                "INSERT INTO transcripts VALUES (?,?,?,?,?)",
                (f"tr-{episode_id}", episode_id, str(path), digest, "ready"),
            )
            conn.execute(
                "INSERT INTO transcript_preparations VALUES (?,?,?,?,?,?)",
                (f"prep-{episode_id}", f"tr-{episode_id}", "prepared", str(path), digest, 1.0),
            )
    return conn


def _ood(root: Path):
    shapes = ["claim_dense", "claim_dense", "narrative", "structural"]
    entries = []
    for show_index, shape in enumerate(shapes):
        episodes = []
        for episode_index in range(4):
            text = "\n\n".join(
                f"Guest: OOD show {show_index} episode {episode_index} turn {turn}. "
                f"Independent sentence marker {turn}."
                for turn in range(90)
            )
            path = root / f"ood-{show_index}-{episode_index}.txt"
            path.write_text(text, encoding="utf-8")
            episodes.append(
                {
                    "episode_id": f"ood-{show_index}-ep-{episode_index}",
                    "episode_title": f"OOD {episode_index}",
                    "transcript_path": str(path),
                }
            )
        entries.append(
            {
                "show_id": f"ood-show-{show_index}",
                "show_name": f"OOD Show {show_index}",
                "source_shape": shape,
                "sealed": True,
                "episodes": episodes,
            }
        )
    return entries


def test_manifest_is_deterministic_hash_frozen_and_has_no_episode_leakage(tmp_path):
    conn = _database(tmp_path)
    first = build_split_manifest(conn, project_root=tmp_path, ood_entries=_ood(tmp_path))
    second = build_split_manifest(conn, project_root=tmp_path, ood_entries=_ood(tmp_path))
    assert first == second
    verify_frozen_manifest(first)
    assert first["counts"]["by_split"] == {
        "development": 3,
        "sealed_holdout": 51,
        "validation": 6,
    }
    episode_splits = {}
    for window in first["windows"]:
        episode_splits.setdefault(window["episode_id"], set()).add(window["split"])
        assert window["char_count"] <= 6_000
        assert "window_text" not in window
    assert all(len(splits) == 1 for splits in episode_splits.values())
    sealed_ood = [w for w in first["windows"] if w["corpus"] == "ood"]
    assert {w["split"] for w in sealed_ood} == {"sealed_holdout"}


def test_duplicate_normalized_feed_is_rejected_instead_of_leaking_alias(tmp_path):
    conn = _database(tmp_path, duplicate_feed=True)
    with pytest.raises(SignalDeskGoldError, match="duplicate canonical RSS feeds"):
        build_split_manifest(conn, project_root=tmp_path)
    manifest = build_split_manifest(
        conn,
        project_root=tmp_path,
        show_aliases={"show-alias": "show-a"},
    )
    assert manifest["counts"]["shows"] == 1
    assert manifest["show_aliases"] == {"show-alias": "show-a"}


def test_campaign_show_counts_are_explicit_not_silently_underfilled(tmp_path):
    conn = _database(tmp_path)
    with pytest.raises(SignalDeskGoldError, match="expected 57"):
        build_split_manifest(
            conn,
            project_root=tmp_path,
            expected_in_domain_shows=57,
        )


def test_private_gold_a_b_c_packets_share_exact_glm_contract_and_sealed_is_guarded(tmp_path):
    conn = _database(tmp_path)
    manifest = build_split_manifest(conn, project_root=tmp_path, ood_entries=_ood(tmp_path))
    by_pass = {
        gold_pass: build_gold_packets(
            manifest, project_root=tmp_path, gold_pass=gold_pass, splits=("development",)
        )
        for gold_pass in "ABC"
    }
    assert all(len(packets) == 3 for packets in by_pass.values())
    schemas = {
        json.dumps(packets[0]["output_schema"], sort_keys=True)
        for packets in by_pass.values()
    }
    assert len(schemas) == 1
    assert {packets[0]["contract_version"] for packets in by_pass.values()} == {
        GLM_CONTRACT_VERSION
    }
    assert all(packets[0]["input"]["window_text"] for packets in by_pass.values())
    with pytest.raises(SignalDeskGoldError, match="explicit authorization"):
        build_gold_packets(
            manifest,
            project_root=tmp_path,
            gold_pass="A",
            splits=("sealed_holdout",),
        )


def test_manifest_hash_detects_tampering(tmp_path):
    conn = _database(tmp_path)
    manifest = build_split_manifest(conn, project_root=tmp_path)
    manifest["windows"][0]["end_char"] += 1
    with pytest.raises(SignalDeskGoldError, match="hash verification failed"):
        verify_frozen_manifest(manifest)


def test_gold_audit_expands_in_blocks_until_event_denominator_is_powered(tmp_path):
    conn = _database(tmp_path)
    manifest = build_split_manifest(conn, project_root=tmp_path, ood_entries=_ood(tmp_path))
    counts = {window["window_id"]: 10 for window in manifest["windows"]}
    audit = select_gold_audit(
        manifest,
        counts,
        initial_windows=20,
        expansion_block=10,
        minimum_events=350,
    )
    assert audit["sample_windows"] == 40
    assert audit["sample_events"] == 400
    assert audit["expanded"] is True
    assert audit["decision_ready"] is True
    assert audit == select_gold_audit(
        manifest,
        counts,
        initial_windows=20,
        expansion_block=10,
        minimum_events=350,
    )
