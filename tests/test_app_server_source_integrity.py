from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from research_factory.app_server_source_integrity import (
    SOURCE_INTEGRITY_VERSION,
    SourceIntegrityError,
    VerifiedSourceLoader,
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _database() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE episodes (
          id TEXT PRIMARY KEY, title TEXT, published_at TEXT
        );
        CREATE TABLE sources (
          id TEXT PRIMARY KEY, name TEXT NOT NULL
        );
        CREATE TABLE transcripts (
          id TEXT PRIMARY KEY, raw_text_path TEXT, raw_text_sha256 TEXT
        );
        CREATE TABLE transcript_preparations (
          id TEXT PRIMARY KEY, transcript_id TEXT NOT NULL,
          cleaned_text_path TEXT, cleaned_text_sha256 TEXT,
          artifact_type TEXT NOT NULL, status TEXT NOT NULL,
          substantive_word_count INTEGER, boilerplate_ratio REAL,
          speaker_turn_count INTEGER, quality_score REAL
        );
        CREATE TABLE segments (
          id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, source_id TEXT NOT NULL,
          transcript_id TEXT NOT NULL, segment_index INTEGER NOT NULL,
          start_char INTEGER NOT NULL, end_char INTEGER NOT NULL,
          text_path TEXT NOT NULL, text_sha256 TEXT NOT NULL
        );
        """
    )
    return connection


def _write(path: Path, text: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = text.encode("utf-8")
    path.write_bytes(raw)
    return _sha256(raw)


def _insert_source(
    connection: sqlite3.Connection,
    root: Path,
    *,
    episode_id: str,
    text: str,
    segment_text: str,
    status: str = "prepared",
    cleaned: bool = True,
    substantive_word_count: int | None = 12,
) -> tuple[Path, Path]:
    source_id = f"source-{episode_id}"
    transcript_id = f"transcript-{episode_id}"
    preparation_id = f"preparation-{episode_id}"
    segment_id = f"segment-{episode_id}"
    raw_path = root / f"{transcript_id}.raw.txt"
    cleaned_path = root / f"{transcript_id}.cleaned.txt"
    segment_path = root / f"{segment_id}.txt"
    raw_sha = _write(raw_path, text)
    cleaned_sha = _write(cleaned_path, text) if cleaned else None
    segment_sha = _write(segment_path, segment_text)
    start = text.index(segment_text)
    end = start + len(segment_text)
    connection.execute(
        "INSERT INTO episodes VALUES (?, 'Fixture episode', '2026-07-18')",
        (episode_id,),
    )
    connection.execute("INSERT INTO sources VALUES (?, 'Fixture source')", (source_id,))
    connection.execute(
        "INSERT INTO transcripts VALUES (?, ?, ?)",
        (transcript_id, str(raw_path), raw_sha),
    )
    connection.execute(
        "INSERT INTO transcript_preparations VALUES (?, ?, ?, ?, 'dialogue_transcript', "
        "?, ?, 0.0, 0, 0.5)",
        (
            preparation_id,
            transcript_id,
            str(cleaned_path) if cleaned else None,
            cleaned_sha,
            status,
            substantive_word_count,
        ),
    )
    connection.execute(
        "INSERT INTO segments VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)",
        (
            segment_id,
            episode_id,
            source_id,
            transcript_id,
            start,
            end,
            str(segment_path),
            segment_sha,
        ),
    )
    connection.commit()
    return cleaned_path if cleaned else raw_path, segment_path


def test_prepared_source_is_hash_and_offset_verified_and_preserves_explicit_zero(tmp_path: Path) -> None:
    connection = _database()
    text = "Intro.\nHost: Exact prepared segment.\nOutro.\n"
    _insert_source(
        connection,
        tmp_path,
        episode_id="prepared",
        text=text,
        segment_text="Host: Exact prepared segment.",
        substantive_word_count=0,
    )
    loader = VerifiedSourceLoader(connection, source_root=tmp_path)

    packet = loader.episode_packet("prepared")

    assert packet["source_integrity"]["schema_version"] == SOURCE_INTEGRITY_VERSION
    assert packet["episode"]["transcript_substantive_word_count"] == 0
    assert packet["source_integrity"]["transcript_source_bases"][0][
        "source_basis"
    ] == "cleaned_preparation"
    segment = packet["source_integrity"]["segments"][0]
    assert text[segment["start_char"] : segment["end_char"]] == (
        "Host: Exact prepared segment."
    )
    connection.close()


def test_explicit_low_signal_without_cleaned_artifact_uses_verified_raw_basis(tmp_path: Path) -> None:
    connection = _database()
    text = "Header.\nHost: Sparse but preserved source.\n"
    _insert_source(
        connection,
        tmp_path,
        episode_id="low-signal",
        text=text,
        segment_text="Host: Sparse but preserved source.",
        status="low_signal",
        cleaned=False,
    )
    loader = VerifiedSourceLoader(connection, source_root=tmp_path)

    packet = loader.episode_packet("low-signal")

    basis = packet["source_integrity"]["transcript_source_bases"][0]
    assert basis["source_basis"] == "raw_transcript_for_low_signal"
    assert basis["transcript_preparation_status"] == "low_signal"
    connection.close()


@pytest.mark.parametrize("tamper_target", ["segment", "prepared"])
def test_source_drift_fails_before_a_packet_can_be_claimed(
    tmp_path: Path, tamper_target: str
) -> None:
    connection = _database()
    source_path, segment_path = _insert_source(
        connection,
        tmp_path,
        episode_id=f"tamper-{tamper_target}",
        text="Host: Immutable source bytes.\n",
        segment_text="Host: Immutable source bytes.",
    )
    target = segment_path if tamper_target == "segment" else source_path
    target.write_text("tampered bytes", encoding="utf-8")
    loader = VerifiedSourceLoader(connection, source_root=tmp_path)

    with pytest.raises(SourceIntegrityError, match="SHA-256 drifted"):
        loader.episode_packet(f"tamper-{tamper_target}")
    connection.close()


def test_segment_offset_substitution_fails_even_when_file_hash_matches(tmp_path: Path) -> None:
    connection = _database()
    _insert_source(
        connection,
        tmp_path,
        episode_id="offset-drift",
        text="Prefix. Host: Exact source bytes. Suffix.",
        segment_text="Host: Exact source bytes.",
    )
    connection.execute(
        "UPDATE segments SET start_char = start_char + 1, end_char = end_char + 1 "
        "WHERE episode_id = 'offset-drift'"
    )
    connection.commit()
    loader = VerifiedSourceLoader(connection, source_root=tmp_path)

    with pytest.raises(SourceIntegrityError, match="do not match.*offsets"):
        loader.episode_packet("offset-drift")
    connection.close()


@pytest.mark.parametrize("tamper_target", ["raw", "cleaned", "segment"])
def test_fresh_revalidation_rejects_postclaim_source_mutation(
    tmp_path: Path, tamper_target: str
) -> None:
    connection = _database()
    low_signal = tamper_target == "raw"
    source_path, segment_path = _insert_source(
        connection,
        tmp_path,
        episode_id=f"postclaim-{tamper_target}",
        text="Header.\nHost: Frozen source after claim.\n",
        segment_text="Host: Frozen source after claim.",
        status="low_signal" if low_signal else "prepared",
        cleaned=not low_signal,
    )
    loader = VerifiedSourceLoader(connection, source_root=tmp_path)
    loader.episode_packet(f"postclaim-{tamper_target}")
    target = segment_path if tamper_target == "segment" else source_path
    target.write_text("postclaim source mutation", encoding="utf-8")

    with pytest.raises(SourceIntegrityError, match="SHA-256 drifted"):
        loader.fresh_episode_packet(f"postclaim-{tamper_target}")
    connection.close()
