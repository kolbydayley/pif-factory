from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .paths import corpus_dir, root
from .util import dumps_json, now_iso, read_text, sha256_text, stable_id, write_text_atomic


ARTIFACT_TYPES = {
    "dialogue_transcript",
    "caption_transcript",
    "article_show_notes",
    "mixed_page",
    "boilerplate",
}

SPEAKER_LINE_RE = re.compile(r"^\s*([A-Z][A-Za-z0-9 ._'&-]{1,48}|[A-Z][A-Z0-9 ._'&-]{1,48}):\s+\S")
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
TIMESTAMP_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b")

BOILERPLATE_PATTERNS = [
    re.compile(pattern, re.I)
    for pattern in [
        r"\bsubscribe\b",
        r"\bsign in\b",
        r"\bshare transcript\b",
        r"\bfull show notes\b",
        r"\bshow notes\b",
        r"\baudio playback is not supported\b",
        r"\bplease upgrade\b",
        r"\bpromo code\b",
        r"\bsponsor\b",
        r"\bfollow us\b",
        r"\bnewsletter\b",
        r"\bcurrent time\b",
        r"\bprivacy policy\b",
        r"\bterms of service\b",
        r"\bdownload mp3\b",
        r"\bapple podcasts\b",
        r"\bspotify\b",
        r"\byoutube\b",
    ]
]

SUBSTANTIVE_AI_RE = re.compile(
    r"\b(agi|agent|agents|ai|model|models|openai|anthropic|google|meta|microsoft|nvidia|"
    r"inference|training|evals?|alignment|safety|coding|compute|gpu|llm|frontier|enterprise)\b",
    re.I,
)


@dataclass(frozen=True)
class PreparationResult:
    artifact_type: str
    status: str
    cleaned_text: str
    original_word_count: int
    substantive_word_count: int
    boilerplate_word_count: int
    boilerplate_ratio: float
    speaker_turn_count: int
    quality_score: float
    notes: dict[str, Any]


def prepare_transcript(conn, transcript_id: str, *, force: bool = False) -> dict[str, Any]:
    existing = conn.execute("SELECT * FROM transcript_preparations WHERE transcript_id = ?", (transcript_id,)).fetchone()
    if existing and not force:
        return _public_existing(existing)
    row = conn.execute(
        """
        SELECT
          transcripts.*,
          episodes.title AS episode_title,
          sources.name AS source_name
        FROM transcripts
        JOIN episodes ON episodes.id = transcripts.episode_id
        JOIN sources ON sources.id = episodes.source_id
        WHERE transcripts.id = ?
        """,
        (transcript_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Transcript not found: {transcript_id}")
    text = read_text(root() / row["raw_text_path"])
    prepared = prepare_transcript_text(
        text,
        content_type=row["content_type"],
        source_kind=row["source_kind"],
        source_name=row["source_name"],
        episode_title=row["episode_title"],
    )
    prep_id = stable_id(transcript_id, row["raw_text_sha256"], "prep_v1", prefix="prep_")
    cleaned_path = None
    cleaned_sha = None
    if prepared.cleaned_text:
        output_path = corpus_dir() / "prepared_transcripts" / f"{prep_id}.txt"
        write_text_atomic(output_path, prepared.cleaned_text)
        cleaned_path = str(output_path.relative_to(corpus_dir().parent))
        cleaned_sha = sha256_text(prepared.cleaned_text)
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO transcript_preparations
          (id, transcript_id, artifact_type, status, cleaned_text_path, cleaned_text_sha256,
           original_word_count, substantive_word_count, boilerplate_word_count, boilerplate_ratio,
           speaker_turn_count, quality_score, notes_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(transcript_id) DO UPDATE SET
          artifact_type = excluded.artifact_type,
          status = excluded.status,
          cleaned_text_path = excluded.cleaned_text_path,
          cleaned_text_sha256 = excluded.cleaned_text_sha256,
          original_word_count = excluded.original_word_count,
          substantive_word_count = excluded.substantive_word_count,
          boilerplate_word_count = excluded.boilerplate_word_count,
          boilerplate_ratio = excluded.boilerplate_ratio,
          speaker_turn_count = excluded.speaker_turn_count,
          quality_score = excluded.quality_score,
          notes_json = excluded.notes_json,
          updated_at = excluded.updated_at
        """,
        (
            prep_id,
            transcript_id,
            prepared.artifact_type,
            prepared.status,
            cleaned_path,
            cleaned_sha,
            prepared.original_word_count,
            prepared.substantive_word_count,
            prepared.boilerplate_word_count,
            prepared.boilerplate_ratio,
            prepared.speaker_turn_count,
            prepared.quality_score,
            dumps_json(prepared.notes),
            ts,
            ts,
        ),
    )
    return {
        "id": prep_id,
        "transcript_id": transcript_id,
        "artifact_type": prepared.artifact_type,
        "status": prepared.status,
        "substantive_word_count": prepared.substantive_word_count,
        "boilerplate_ratio": prepared.boilerplate_ratio,
        "speaker_turn_count": prepared.speaker_turn_count,
        "quality_score": prepared.quality_score,
    }


def prepare_transcript_text(
    text: str,
    *,
    content_type: str | None,
    source_kind: str | None,
    source_name: str | None = None,
    episode_title: str | None = None,
) -> PreparationResult:
    lines = [line.strip() for line in text.splitlines()]
    non_empty = [line for line in lines if line]
    original_word_count = len(text.split())
    speaker_turn_count = sum(1 for line in non_empty if SPEAKER_LINE_RE.match(line))
    timestamp_count = sum(1 for line in non_empty if TIMESTAMP_RE.search(line))
    url_count = len(URL_RE.findall(text))
    show_chrome_hits = sum(_is_boilerplate_line(line, source_name=source_name, episode_title=episode_title) for line in non_empty)
    cleaned_lines = []
    removed_word_count = 0
    for line in non_empty:
        if _is_boilerplate_line(line, source_name=source_name, episode_title=episode_title):
            removed_word_count += len(line.split())
            continue
        cleaned = URL_RE.sub("", line).strip()
        if cleaned:
            cleaned_lines.append(cleaned)
    cleaned_text = "\n".join(_dedupe_adjacent(cleaned_lines)).strip()
    substantive_word_count = len(cleaned_text.split())
    boilerplate_word_count = max(original_word_count - substantive_word_count, removed_word_count)
    boilerplate_ratio = round(boilerplate_word_count / max(original_word_count, 1), 4)
    artifact_type = _classify_artifact(
        original_word_count=original_word_count,
        substantive_word_count=substantive_word_count,
        speaker_turn_count=speaker_turn_count,
        timestamp_count=timestamp_count,
        url_count=url_count,
        show_chrome_hits=show_chrome_hits,
        content_type=content_type,
        source_kind=source_kind,
        text=text,
    )
    status = "low_signal" if artifact_type == "boilerplate" or substantive_word_count < 50 else "prepared"
    quality_score = _quality_score(
        artifact_type=artifact_type,
        substantive_word_count=substantive_word_count,
        boilerplate_ratio=boilerplate_ratio,
        speaker_turn_count=speaker_turn_count,
    )
    notes = {
        "content_type": content_type,
        "source_kind": source_kind,
        "line_count": len(non_empty),
        "removed_line_count": max(len(non_empty) - len(cleaned_lines), 0),
        "timestamp_count": timestamp_count,
        "url_count": url_count,
        "show_chrome_hits": show_chrome_hits,
        "method": "heuristic_prepare_transcript_v1",
    }
    return PreparationResult(
        artifact_type=artifact_type,
        status=status,
        cleaned_text=cleaned_text,
        original_word_count=original_word_count,
        substantive_word_count=substantive_word_count,
        boilerplate_word_count=boilerplate_word_count,
        boilerplate_ratio=boilerplate_ratio,
        speaker_turn_count=speaker_turn_count,
        quality_score=quality_score,
        notes=notes,
    )


def prepared_text_for_transcript(conn, transcript_id: str) -> tuple[str | None, dict[str, Any] | None]:
    row = conn.execute("SELECT * FROM transcript_preparations WHERE transcript_id = ?", (transcript_id,)).fetchone()
    if not row or not row["cleaned_text_path"]:
        return None, None
    return read_text(root() / row["cleaned_text_path"]), _public_existing(row)


def _classify_artifact(
    *,
    original_word_count: int,
    substantive_word_count: int,
    speaker_turn_count: int,
    timestamp_count: int,
    url_count: int,
    show_chrome_hits: int,
    content_type: str | None,
    source_kind: str | None,
    text: str,
) -> str:
    if original_word_count < 80 and speaker_turn_count < 2:
        return "boilerplate"
    if substantive_word_count < 35:
        return "boilerplate"
    source_kind = (source_kind or "").lower()
    content_type = (content_type or "").lower()
    chrome_heavy = show_chrome_hits >= 4 or url_count >= 8 or "audio playback is not supported" in text.lower()
    if speaker_turn_count >= 2:
        return "mixed_page" if chrome_heavy else "dialogue_transcript"
    if chrome_heavy:
        return "mixed_page" if substantive_word_count >= 120 else "boilerplate"
    if source_kind == "youtube_captions" or "vtt" in content_type or "srt" in content_type:
        return "caption_transcript"
    if timestamp_count >= 8:
        return "caption_transcript"
    if "transcript" in text.lower() and SUBSTANTIVE_AI_RE.search(text):
        return "article_show_notes"
    return "caption_transcript"


def _quality_score(*, artifact_type: str, substantive_word_count: int, boilerplate_ratio: float, speaker_turn_count: int) -> float:
    score = 0.35
    if artifact_type in {"dialogue_transcript", "caption_transcript"}:
        score += 0.35
    elif artifact_type == "mixed_page":
        score += 0.2
    elif artifact_type == "article_show_notes":
        score += 0.12
    if substantive_word_count >= 400:
        score += 0.15
    elif substantive_word_count >= 120:
        score += 0.08
    if speaker_turn_count >= 4:
        score += 0.08
    score -= min(boilerplate_ratio, 0.8) * 0.35
    return round(max(0.0, min(1.0, score)), 3)


def _is_boilerplate_line(line: str, *, source_name: str | None, episode_title: str | None) -> bool:
    lowered = line.lower().strip()
    if not lowered:
        return True
    if source_name and lowered == source_name.lower().strip():
        return True
    if episode_title and lowered == episode_title.lower().strip():
        return True
    if len(line.split()) <= 3 and any(token in lowered for token in ["1x", "0:00", "share", "like", "comment"]):
        return True
    if re.fullmatch(r"[\d\s:./-]+", lowered):
        return True
    if any(pattern.search(line) for pattern in BOILERPLATE_PATTERNS):
        return len(line.split()) < 45 or not SUBSTANTIVE_AI_RE.search(line)
    return False


def _dedupe_adjacent(lines: list[str]) -> list[str]:
    deduped = []
    previous = None
    for line in lines:
        normalized = re.sub(r"\s+", " ", line.lower()).strip()
        if normalized and normalized != previous:
            deduped.append(line)
        previous = normalized
    return deduped


def _public_existing(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "transcript_id": row["transcript_id"],
        "artifact_type": row["artifact_type"],
        "status": row["status"],
        "substantive_word_count": row["substantive_word_count"],
        "boilerplate_ratio": row["boilerplate_ratio"],
        "speaker_turn_count": row["speaker_turn_count"],
        "quality_score": row["quality_score"],
    }
