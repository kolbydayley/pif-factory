from __future__ import annotations

"""Hash- and offset-verified source loading for managed app-server lanes."""

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from .paths import corpus_dir


SOURCE_INTEGRITY_VERSION = "pif_app_server_source_integrity_v1"


class SourceIntegrityError(RuntimeError):
    pass


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


class VerifiedSourceLoader:
    """Read each transcript basis once and verify every segment against it."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        source_root: Path | None = None,
    ) -> None:
        self.conn = conn
        self.source_root = (
            source_root.expanduser().resolve()
            if source_root is not None
            else corpus_dir().parent.resolve()
        )
        self._text_cache: dict[
            tuple[str, int, int, int, int, str], str
        ] = {}

    def _resolve(self, value: Any, *, label: str) -> Path:
        if not isinstance(value, str) or not value:
            raise SourceIntegrityError(f"{label} path is missing")
        candidate = Path(value).expanduser()
        path = (
            candidate.resolve()
            if candidate.is_absolute()
            else (self.source_root / candidate).resolve()
        )
        try:
            path.relative_to(self.source_root)
        except ValueError as exc:
            raise SourceIntegrityError(f"{label} path escaped the source root") from exc
        return path

    def _verified_text(
        self, path_value: Any, expected_hash: Any, *, label: str
    ) -> tuple[str, dict[str, Any]]:
        if not _is_sha256(expected_hash):
            raise SourceIntegrityError(f"{label} expected SHA-256 is invalid")
        path = self._resolve(path_value, label=label)
        try:
            stat = path.stat()
        except OSError as exc:
            raise SourceIntegrityError(f"{label} file is unavailable") from exc
        if not path.is_file():
            raise SourceIntegrityError(f"{label} file is unavailable")
        identity = (
            str(path),
            int(stat.st_dev),
            int(stat.st_ino),
            int(stat.st_size),
            int(stat.st_mtime_ns),
            str(expected_hash),
        )
        text = self._text_cache.get(identity)
        if text is None:
            try:
                raw = path.read_bytes()
                text = raw.decode("utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                raise SourceIntegrityError(f"{label} file is unreadable") from exc
            if _sha256_bytes(raw) != expected_hash:
                raise SourceIntegrityError(f"{label} SHA-256 drifted")
            self._text_cache[identity] = text
        return text, {
            "path": str(path),
            "expected_sha256": str(expected_hash),
            "size_bytes": int(stat.st_size),
            "device": int(stat.st_dev),
            "inode": int(stat.st_ino),
            "mtime_ns": int(stat.st_mtime_ns),
        }

    def _row(self, segment_id: str) -> sqlite3.Row:
        row = self.conn.execute(
            """
            SELECT
              segments.*,
              episodes.title AS episode_title,
              episodes.published_at AS episode_published_at,
              sources.name AS source_name,
              transcripts.raw_text_path,
              transcripts.raw_text_sha256,
              transcript_preparations.id AS transcript_preparation_id,
              transcript_preparations.cleaned_text_path,
              transcript_preparations.cleaned_text_sha256,
              transcript_preparations.artifact_type AS transcript_artifact_type,
              transcript_preparations.status AS transcript_preparation_status,
              transcript_preparations.substantive_word_count AS transcript_substantive_word_count,
              transcript_preparations.boilerplate_ratio AS transcript_boilerplate_ratio,
              transcript_preparations.speaker_turn_count AS transcript_speaker_turn_count,
              transcript_preparations.quality_score AS transcript_quality_score
            FROM segments
            JOIN episodes ON episodes.id = segments.episode_id
            JOIN sources ON sources.id = segments.source_id
            JOIN transcripts ON transcripts.id = segments.transcript_id
            JOIN transcript_preparations
              ON transcript_preparations.transcript_id = segments.transcript_id
            WHERE segments.id = ?
            """,
            (segment_id,),
        ).fetchone()
        if row is None:
            raise SourceIntegrityError(f"verified segment is unavailable: {segment_id}")
        return row

    def _source_basis(
        self, row: Mapping[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        cleaned_path = row["cleaned_text_path"]
        cleaned_hash = row["cleaned_text_sha256"]
        if bool(cleaned_path) != bool(cleaned_hash):
            raise SourceIntegrityError(
                "prepared transcript path/hash completeness drifted"
            )
        if cleaned_path:
            text, file_record = self._verified_text(
                cleaned_path,
                cleaned_hash,
                label="prepared transcript",
            )
            basis = "cleaned_preparation"
            path = str(cleaned_path)
            digest = str(cleaned_hash)
        else:
            if str(row["transcript_preparation_status"]) != "low_signal":
                raise SourceIntegrityError(
                    "missing prepared transcript is not an explicit low-signal case"
                )
            text, file_record = self._verified_text(
                row["raw_text_path"],
                row["raw_text_sha256"],
                label="low-signal raw transcript",
            )
            basis = "raw_transcript_for_low_signal"
            path = str(row["raw_text_path"])
            digest = str(row["raw_text_sha256"])
        return text, {
            "transcript_id": str(row["transcript_id"]),
            "transcript_preparation_id": str(row["transcript_preparation_id"]),
            "transcript_preparation_status": str(
                row["transcript_preparation_status"]
            ),
            "source_basis": basis,
            "source_path": path,
            "source_sha256": digest,
            "verified_file": file_record,
        }

    def _verified_row(
        self, row: Mapping[str, Any]
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        source_text, source_record = self._source_basis(row)
        segment_text, segment_record = self._verified_text(
            row["text_path"],
            row["text_sha256"],
            label="segment",
        )
        try:
            start = int(row["start_char"])
            end = int(row["end_char"])
        except (TypeError, ValueError) as exc:
            raise SourceIntegrityError("segment offsets are invalid") from exc
        if not 0 <= start <= end <= len(source_text):
            raise SourceIntegrityError("segment offsets escaped the transcript source basis")
        if source_text[start:end] != segment_text:
            raise SourceIntegrityError(
                "segment bytes do not match transcript source-basis offsets"
            )
        segment = {
            "segment_id": str(row["id"]),
            "transcript_id": str(row["transcript_id"]),
            "segment_index": int(row["segment_index"]),
            "start_char": start,
            "end_char": end,
            "text_path": str(row["text_path"]),
            "text_sha256": str(row["text_sha256"]),
            "verified_file": segment_record,
        }
        return segment_text, source_record, segment

    def segment_packet(self, segment_id: str) -> dict[str, Any]:
        row = self._row(segment_id)
        segment_text, source_basis, segment = self._verified_row(row)
        context = {
            "segment_id": str(row["id"]),
            "episode_id": str(row["episode_id"]),
            "source_id": str(row["source_id"]),
            "source_name": str(row["source_name"]),
            "episode_title": str(row["episode_title"] or ""),
            "episode_published_at": row["episode_published_at"],
            "segment_index": int(row["segment_index"]),
            "start_char": int(row["start_char"]),
            "end_char": int(row["end_char"]),
            "transcript_preparation_id": str(row["transcript_preparation_id"]),
            "transcript_artifact_type": str(row["transcript_artifact_type"]),
            "transcript_preparation_status": str(row["transcript_preparation_status"]),
            "transcript_substantive_word_count": row[
                "transcript_substantive_word_count"
            ],
            "transcript_boilerplate_ratio": row["transcript_boilerplate_ratio"],
            "transcript_speaker_turn_count": row["transcript_speaker_turn_count"],
            "transcript_quality_score": row["transcript_quality_score"],
            "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
        }
        return {
            "context": context,
            "segment_text": segment_text,
            "source_integrity": {
                "schema_version": SOURCE_INTEGRITY_VERSION,
                "episode_id": str(row["episode_id"]),
                "transcript_source_basis": source_basis,
                "segment": segment,
            },
        }

    def episode_packet(self, episode_id: str) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT segments.id
            FROM segments
            WHERE segments.episode_id = ?
            ORDER BY segments.transcript_id, segments.segment_index
            """,
            (episode_id,),
        ).fetchall()
        if not rows:
            raise SourceIntegrityError(
                f"no prepared segments found for episode: {episode_id}"
            )
        packets = [self.segment_packet(str(row["id"])) for row in rows]
        contexts = [packet["context"] for packet in packets]
        if any(context["episode_id"] != episode_id for context in contexts):
            raise SourceIntegrityError("episode source packet crossed episode scope")
        parts: list[str] = []
        total_words = 0
        sources_by_transcript: dict[str, dict[str, Any]] = {}
        segments: list[dict[str, Any]] = []
        for packet in packets:
            context = packet["context"]
            text = packet["segment_text"]
            segment = packet["source_integrity"]["segment"]
            source_basis = packet["source_integrity"]["transcript_source_basis"]
            sources_by_transcript[source_basis["transcript_id"]] = source_basis
            segments.append(segment)
            total_words += len(text.split())
            parts.append(
                "\n".join(
                    [
                        (
                            f"===== SEGMENT {context['segment_index']} | "
                            f"{context['segment_id']} | chars={context['start_char']}-"
                            f"{context['end_char']} | words={len(text.split())} ====="
                        ),
                        text,
                    ]
                )
            )
        first = contexts[0]
        integrity = {
            "schema_version": SOURCE_INTEGRITY_VERSION,
            "episode_id": episode_id,
            "transcript_source_bases": [
                sources_by_transcript[key] for key in sorted(sources_by_transcript)
            ],
            "segments": segments,
        }
        integrity["source_integrity_sha256"] = _sha256_bytes(
            json.dumps(
                integrity,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        first_source = integrity["transcript_source_bases"][0]
        return {
            "episode": {
                "episode_id": episode_id,
                "transcript_id": first_source["transcript_id"],
                "source_id": first["source_id"],
                "source_name": first["source_name"],
                "episode_title": first["episode_title"],
                "episode_published_at": first["episode_published_at"],
                "transcript_preparation_id": first["transcript_preparation_id"],
                "transcript_artifact_type": first["transcript_artifact_type"],
                "transcript_preparation_status": first[
                    "transcript_preparation_status"
                ],
                "transcript_substantive_word_count": first[
                    "transcript_substantive_word_count"
                ],
                "transcript_boilerplate_ratio": first[
                    "transcript_boilerplate_ratio"
                ],
                "transcript_speaker_turn_count": first[
                    "transcript_speaker_turn_count"
                ],
                "transcript_quality_score": first["transcript_quality_score"],
                "segment_count": len(packets),
                "total_segment_words": total_words,
                "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
            },
            "full_segmented_episode_text": "\n\n".join(parts),
            "source_integrity": integrity,
        }

    def fresh_episode_packet(self, episode_id: str) -> dict[str, Any]:
        """Re-read every source artifact without reusing this instance's cache."""

        return type(self)(self.conn, source_root=self.source_root).episode_packet(
            episode_id
        )


__all__ = [
    "SOURCE_INTEGRITY_VERSION",
    "SourceIntegrityError",
    "VerifiedSourceLoader",
]
