import json
import sqlite3
from pathlib import Path

import pytest

from research_factory.signal_desk_rebuild_gold import (
    GLM_CONTRACT_VERSION,
    SignalDeskGoldError,
    assemble_complete_frozen_manifest,
    build_gold_packets,
    build_split_manifest,
    freeze_per_show_artifacts,
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
          id TEXT PRIMARY KEY, source_id TEXT, guid TEXT, title TEXT, published_at TEXT,
          duration_seconds REAL
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
    dates = ["2025-01-01", "2025-04-01", "2025-07-01", "2025-10-01", "2026-01-01"]
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
                "INSERT INTO episodes VALUES (?,?,?,?,?,?)",
                (episode_id, source_id, f"guid-{index}", f"Episode {index}", dates[index], 600),
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
    shapes = ["claim_dense", "claim_dense", "narrative", "format_stress"]
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
                    "published_at": ["2025-01-01", "2025-04-01", "2025-07-01", "2025-10-01"][episode_index],
                    "duration_seconds": 600,
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


def test_sources_with_no_ready_transcripts_are_reported_not_silently_absent(tmp_path):
    conn = _database(tmp_path)
    conn.execute(
        "INSERT INTO sources VALUES (?,?,?,?)",
        ("show-empty", "Empty Show", "https://feeds.example/empty", 1),
    )
    manifest = build_split_manifest(conn, project_root=tmp_path)
    diagnostic = next(
        row for row in manifest["coverage_diagnostics"]
        if row["show_id"] == "show-empty"
    )
    assert diagnostic["covered"] is False
    assert diagnostic["blocking_reasons"] == ["no_ready_local_transcripts"]


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


def test_coverage_requires_temporal_breadth_and_reports_reason(tmp_path):
    conn = _database(tmp_path)
    conn.execute("UPDATE episodes SET published_at = '2026-08-01'")
    manifest = build_split_manifest(conn, project_root=tmp_path, ood_entries=_ood(tmp_path))
    diagnostic = next(row for row in manifest["coverage_diagnostics"] if row["show_id"] == "show-a")
    assert diagnostic["covered"] is False
    assert "insufficient_distinct_publication_months" in diagnostic["blocking_reasons"]
    assert "insufficient_publication_span" in diagnostic["blocking_reasons"]


def test_duration_plausibility_and_flattened_transcripts_are_a_stratum(tmp_path):
    conn = _database(tmp_path)
    # Three episodes fail the shell-page plausibility guard. The remaining two
    # cannot satisfy the four-episode coverage contract.
    conn.execute("UPDATE episodes SET duration_seconds = 100000 WHERE id IN ('show-a-ep-0','show-a-ep-1','show-a-ep-2')")
    manifest = build_split_manifest(conn, project_root=tmp_path, ood_entries=_ood(tmp_path))
    diagnostic = next(row for row in manifest["coverage_diagnostics"] if row["show_id"] == "show-a")
    assert diagnostic["covered"] is False
    assert diagnostic["rejection_counts"]["implausible_word_count_for_duration"] == 3

    (tmp_path / "flat").mkdir()
    conn = _database(tmp_path / "flat")
    for index in range(5):
        path = tmp_path / "flat" / f"show-a-ep-{index}.txt"
        text = " ".join(
            f"Flattened caption sentence {turn}. Enough independent words here."
            for turn in range(90)
        )
        path.write_text(text, encoding="utf-8")
        digest = __import__("hashlib").sha256(text.encode()).hexdigest()
        conn.execute(
            "UPDATE transcripts SET raw_text_sha256=? WHERE episode_id=?",
            (digest, f"show-a-ep-{index}"),
        )
        conn.execute(
            "UPDATE transcript_preparations SET cleaned_text_sha256=? WHERE transcript_id=?",
            (digest, f"tr-show-a-ep-{index}"),
        )
    manifest = build_split_manifest(conn, project_root=tmp_path / "flat", ood_entries=_ood(tmp_path))
    diagnostic = next(row for row in manifest["coverage_diagnostics"] if row["show_id"] == "show-a")
    assert diagnostic["covered"] is True
    flat_windows = [row for row in manifest["windows"] if row["show_id"] == "show-a"]
    assert {row["transcript_structure"] for row in flat_windows} == {"flattened"}
    assert manifest["counts"]["by_transcript_structure"]["flattened"] == 12
    packet = build_gold_packets(
        manifest,
        project_root=tmp_path / "flat",
        gold_pass="A",
        splits=("development",),
    )[0]
    assert packet["input"]["transcript_structure"] == "flattened"
    assert "never infer a speaker" in packet["input"]["attribution_instruction"]


def test_stale_hash_requires_explicit_refreeze_and_binds_current_revision(tmp_path):
    conn = _database(tmp_path)
    path = tmp_path / "show-a-ep-0.txt"
    path.write_text(path.read_text(encoding="utf-8") + "\nCurrent revision.", encoding="utf-8")

    blocked = build_split_manifest(conn, project_root=tmp_path)
    diagnostic = next(row for row in blocked["coverage_diagnostics"] if row["show_id"] == "show-a")
    assert any(key.startswith("stale_transcript_hash:") for key in diagnostic["rejection_counts"])

    frozen = build_split_manifest(
        conn,
        project_root=tmp_path,
        refreeze_current_transcript_bytes=True,
    )
    selected = [row for row in frozen["windows"] if row["episode_id"] == "show-a-ep-0"]
    assert selected
    for window in selected:
        revision = window["transcript_revision"]
        assert revision["stale_hash_refrozen"] is True
        assert revision["recorded_sha256"] != revision["frozen_sha256"]
        assert revision["frozen_sha256"] == window["transcript_sha256"]


def test_partial_show_artifact_is_frozen_but_cannot_start_tournament(tmp_path):
    conn = _database(tmp_path)
    manifest = build_split_manifest(conn, project_root=tmp_path)
    artifacts = freeze_per_show_artifacts(
        manifest,
        outputs_metadata={"show-a": {"gold_A_receipt_sha256": "a" * 64}},
    )
    assert len(artifacts) == 1
    assert manifest["complete_benchmark"] is False
    assert manifest["tournament_allowed"] is False
    assert manifest["dev_error_reading_allowed"] is False
    artifact = artifacts[0]
    assert len(artifact["episode_ids"]) == 4
    assert len(artifact["windows"]) == 12
    assert artifact["outputs_metadata"]["gold_A_receipt_sha256"] == "a" * 64
    assert artifact["authoring_allowed"] is True
    assert artifact["tournament_allowed"] is False
    assert artifact["dev_error_reading_allowed"] is False
    with pytest.raises(SignalDeskGoldError, match=r"57 current \+ 10 OOD"):
        assemble_complete_frozen_manifest(artifacts)


def test_private_browser_overlay_adds_coverage_without_canonical_transcript_rows(tmp_path):
    conn = _database(tmp_path)
    conn.execute(
        "INSERT INTO sources VALUES (?,?,?,?)",
        ("show-b", "Show B", "https://feeds.example/b", 1),
    )
    episodes = []
    for index, published_at in enumerate(
        ("2025-01-01", "2025-04-01", "2025-07-01", "2025-10-01")
    ):
        episode_id = f"show-b-ep-{index}"
        text = "\n\n".join(
            f"Host: Browser transcript {index}, turn {turn}, with enough exact words."
            for turn in range(90)
        )
        path = tmp_path / f"{episode_id}.txt"
        path.write_text(text, encoding="utf-8")
        digest = __import__("hashlib").sha256(text.encode()).hexdigest()
        conn.execute(
            "INSERT INTO episodes VALUES (?,?,?,?,?,?)",
            (episode_id, "show-b", f"show-b-guid-{index}", f"B {index}", published_at, 600),
        )
        episodes.append(
            {
                "episode_id": episode_id,
                "transcript_path": str(path),
                "transcript_sha256": digest,
            }
        )
    manifest = build_split_manifest(
        conn,
        project_root=tmp_path,
        in_domain_entries=({"source_id": "show-b", "episodes": episodes},),
    )
    assert manifest["counts"]["shows"] == 2
    assert {show["show_id"] for show in manifest["shows"]} == {"show-a", "show-b"}
    assert conn.execute(
        "SELECT COUNT(*) FROM transcripts WHERE episode_id LIKE 'show-b-%'"
    ).fetchone()[0] == 0
