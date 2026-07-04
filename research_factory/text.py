from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from typing import Any


TIMESTAMP_RE = re.compile(r"^\s*(\d{1,2}:)?\d{1,2}:\d{2}(?:[,.]\d{1,3})?\s*(-->\s*.*)?$")
TAG_RE = re.compile(r"<[^>]+>")
JSON_TIMING_RE = re.compile(r'"(?:start|end|text|words)"\s*:', re.I)
CAPTION_ARROW_RE = re.compile(r"\d{1,2}:\d{2}(?::\d{2})?(?:[,.]\d{1,3})?\s*-->\s*\d{1,2}:\d{2}", re.I)
INLINE_TIMESTAMP_RE = re.compile(r"\b\d{1,2}:\d{2}:\d{2}(?:[,.]\d{1,3})?\b")


def clean_whitespace(text: str) -> str:
    text = text.replace("\ufeff", "")
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def transcript_to_text(body: str, content_type: str | None = None, source_url: str | None = None) -> str:
    kind = f"{content_type or ''} {source_url or ''}".lower()
    if _looks_like_json_transcript(kind, body):
        try:
            parsed = json.loads(body)
            return clean_whitespace(_json_transcript_to_text(parsed))
        except Exception:
            pass
    if _looks_like_caption_transcript(kind, body):
        return clean_whitespace(_caption_to_text(body))
    if "html" in kind or "<html" in body[:500].lower():
        without_scripts = re.sub(r"<(script|style).*?</\1>", " ", body, flags=re.I | re.S)
        return clean_whitespace(TAG_RE.sub(" ", without_scripts))
    return clean_whitespace(body)


def _looks_like_json_transcript(kind: str, body: str) -> bool:
    stripped = body.lstrip()
    if "json" in kind:
        return True
    if not stripped or stripped[0] not in "[{":
        return False
    return bool(JSON_TIMING_RE.search(stripped[:5000]))


def _looks_like_caption_transcript(kind: str, body: str) -> bool:
    if any(token in kind for token in [".vtt", ".srt", "text/vtt", "application/x-subrip", "subrip", "captions"]):
        return True
    head = body.lstrip()[:5000]
    if head.upper().startswith("WEBVTT"):
        return True
    return bool(CAPTION_ARROW_RE.search(head))


def _caption_to_text(body: str) -> str:
    lines = []
    seen = set()
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line == "WEBVTT" or TIMESTAMP_RE.match(line) or line.isdigit():
            continue
        line = TAG_RE.sub("", line)
        if line and line not in seen:
            seen.add(line)
            lines.append(line)
    return "\n".join(lines)


def _json_transcript_to_text(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("body") or item.get("transcript") or ""))
            else:
                parts.append(_json_transcript_to_text(item))
        return "\n".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ["text", "body", "transcript"]:
            if isinstance(value.get(key), str):
                return value[key]
        parts = []
        for item in value.values():
            if isinstance(item, (str, list, dict)):
                rendered = _json_transcript_to_text(item)
                if rendered:
                    parts.append(rendered)
        return "\n".join(parts)
    return ""


@dataclass(frozen=True)
class TranscriptQuality:
    ok: bool
    quarantine: bool
    issues: list[str]
    metrics: dict[str, Any]


def analyze_transcript_quality(text: str, *, content_type: str | None = None, source_url: str | None = None) -> TranscriptQuality:
    lines = text.splitlines()
    stripped = text.lstrip()
    word_count = len(text.split())
    timestamp_lines = sum(1 for line in lines if TIMESTAMP_RE.match(line))
    arrow_lines = sum(1 for line in lines if "-->" in line)
    numeric_lines = sum(1 for line in lines if line.strip().isdigit())
    inline_timestamps = len(INLINE_TIMESTAMP_RE.findall(text[:100_000]))
    json_timing_markers = len(JSON_TIMING_RE.findall(text[:100_000]))
    issues: list[str] = []

    if stripped.startswith(("[", "{")) and json_timing_markers >= 3:
        issues.append("raw_json_timing_payload")
    if arrow_lines >= 3 or timestamp_lines >= 5:
        issues.append("caption_timestamp_residue")
    if "WEBVTT" in text[:500] and arrow_lines:
        issues.append("webvtt_residue")
    if numeric_lines >= 20 and timestamp_lines >= 5:
        issues.append("srt_sequence_residue")
    if inline_timestamps >= 20 and word_count > 500:
        issues.append("inline_timestamp_residue")
    if word_count > 50_000 and json_timing_markers >= 10:
        issues.append("implausible_word_count_with_json_markers")

    metrics = {
        "content_type": content_type,
        "source_url_hint": _source_url_hint(source_url),
        "word_count": word_count,
        "line_count": len(lines),
        "timestamp_lines": timestamp_lines,
        "arrow_lines": arrow_lines,
        "numeric_lines": numeric_lines,
        "inline_timestamps": inline_timestamps,
        "json_timing_markers": json_timing_markers,
    }
    quarantine = any(
        issue
        in {
            "raw_json_timing_payload",
            "caption_timestamp_residue",
            "webvtt_residue",
            "srt_sequence_residue",
            "inline_timestamp_residue",
            "implausible_word_count_with_json_markers",
        }
        for issue in issues
    )
    return TranscriptQuality(ok=not issues, quarantine=quarantine, issues=issues, metrics=metrics)


def _source_url_hint(source_url: str | None) -> str | None:
    if not source_url:
        return None
    return re.sub(r"\?.*$", "", source_url)[-120:]


@dataclass(frozen=True)
class Segment:
    index: int
    start_char: int
    end_char: int
    text: str
    word_count: int


def segment_text(text: str, *, max_words: int = 800, overlap_words: int = 80) -> list[Segment]:
    words = list(re.finditer(r"\S+", text))
    if not words:
        return []
    segments: list[Segment] = []
    index = 0
    start_word = 0
    while start_word < len(words):
        end_word = min(start_word + max_words, len(words))
        start_char = words[start_word].start()
        end_char = words[end_word - 1].end()
        segment = text[start_char:end_char].strip()
        segments.append(
            Segment(
                index=index,
                start_char=start_char,
                end_char=end_char,
                text=segment,
                word_count=end_word - start_word,
            )
        )
        if end_word == len(words):
            break
        start_word = max(end_word - overlap_words, start_word + 1)
        index += 1
    return segments
