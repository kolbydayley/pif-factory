from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any


TIMESTAMP_RE = re.compile(r"^\s*(\d{1,2}:)?\d{1,2}:\d{2}(?:[,.]\d{1,3})?\s*(-->\s*.*)?$")
TAG_RE = re.compile(r"<[^>]+>")
JSON_TIMING_RE = re.compile(r'"(?:start|end|text|words)"\s*:', re.I)
CAPTION_ARROW_RE = re.compile(r"\d{1,2}:\d{2}(?::\d{2})?(?:[,.]\d{1,3})?\s*-->\s*\d{1,2}:\d{2}", re.I)
INLINE_TIMESTAMP_RE = re.compile(r"\b\d{1,2}:\d{2}:\d{2}(?:[,.]\d{1,3})?\b")
SHORT_OR_INLINE_TIMESTAMP_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?(?:[,.]\d{1,3})?\b")
PDF_OBJECT_RE = re.compile(r"\b\d+\s+\d+\s+obj\b")


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
        official_section = _official_html_transcript_to_text(kind, body)
        if official_section:
            return clean_whitespace(_strip_timestamp_residue_if_needed(kind, official_section))
        if _requires_structured_html_transcript(kind):
            return ""
        without_scripts = re.sub(r"<(script|style).*?</\1>", " ", body, flags=re.I | re.S)
        text = TAG_RE.sub(" ", without_scripts)
        return clean_whitespace(_strip_timestamp_residue_if_needed(kind, text))
    return clean_whitespace(_strip_timestamp_residue_if_needed(kind, body))


def _strip_timestamp_residue_if_needed(kind: str, body: str) -> str:
    if _looks_like_inline_timestamped_transcript(kind, body):
        return _strip_inline_timestamps(body)
    return clean_whitespace(body)


def _official_html_transcript_to_text(kind: str, body: str) -> str:
    if "theverge.com/" in kind:
        text = _verge_article_body_to_text(body)
        if len(text.split()) >= 1000:
            return text
    if "gcppodcast.com/" in kind:
        text = _strip_standalone_timestamps(_html_id_text(body, "transcript"))
        if len(text.split()) >= 100:
            return text
    if "corecursive.com/" in kind:
        text = _strip_inline_timestamps(_html_class_text(body, "transcript"))
        if len(text.split()) >= 100:
            return text
    if _looks_like_substack_transcript_page(kind):
        text = _substack_transcript_section_to_text(body)
        if len(text.split()) >= 100:
            return text
    if "econtalk.org/" in kind:
        text = _strip_standalone_timestamps(_html_class_text(body, "audio-highlight"))
        if len(text.split()) >= 100:
            return text
    if "cognitiverevolution.ai/" in kind:
        text = _strip_inline_timestamps(_html_class_text(body, "podcast-transcript"))
        if len(text.split()) >= 100:
            return text
        text = _strip_inline_timestamps(
            _html_after_id_text(
                body,
                "transcript",
                stop_classes={"post-tags", "post-toc", "post-feed", "related-posts", "subscribe-cta__content"},
            )
        )
        if len(text.split()) >= 100:
            return text
    return ""


def _requires_structured_html_transcript(kind: str) -> bool:
    return any(
        host in kind
        for host in [
            "econtalk.org/",
            "stackoverflow.blog/",
            "the-stack-overflow-podcast.simplecast.com/",
            "theverge.com/",
            "gcppodcast.com/",
            "corecursive.com/",
            "latent.space/",
            "thegradientpub.substack.com/",
            "newsletter.pragmaticengineer.com/",
        ]
    )


def _verge_article_body_to_text(body: str) -> str:
    for script in re.findall(r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>", body, flags=re.I | re.S):
        script_text = html.unescape(script).strip()
        if not script_text:
            continue
        try:
            payload = json.loads(script_text)
        except Exception:
            continue
        for article_body in _iter_jsonld_article_bodies(payload):
            text = clean_whitespace(str(article_body))
            if text:
                return _strip_timestamp_residue_if_needed("theverge.com", text)
    return ""


def _iter_jsonld_article_bodies(value: Any):
    if isinstance(value, dict):
        article_body = value.get("articleBody")
        if isinstance(article_body, str):
            yield article_body
        for nested in value.values():
            yield from _iter_jsonld_article_bodies(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_jsonld_article_bodies(item)


def _looks_like_substack_transcript_page(kind: str) -> bool:
    return any(
        host in kind
        for host in [
            "latent.space/",
            "thegradientpub.substack.com/",
            "newsletter.pragmaticengineer.com/",
            "lastweekin.ai/",
            "bigtechnologypodcast.substack.com/",
        ]
    )


def _substack_transcript_section_to_text(body: str) -> str:
    without_scripts = re.sub(r"<(script|style).*?</\1>", " ", body, flags=re.I | re.S)
    text = TAG_RE.sub(" ", without_scripts)
    text = clean_whitespace(text)
    marker = re.search(r"\bTranscript\b", text, flags=re.I)
    if not marker:
        return ""
    text = text[marker.end() :]
    stop = re.search(
        r"\b(Subscribe|Comments|Discussion about this post|Share this post|Listen to this episode|Full episode|Thanks for reading)\b",
        text,
        flags=re.I,
    )
    if stop:
        text = text[: stop.start()]
    return _strip_timestamp_residue_if_needed("", text)


def _strip_standalone_timestamps(text: str) -> str:
    text = re.sub(
        r"\\?[\[(][^\]\)]{0,80}?\\*\d{1,2}:\d{2}(?::\d{2})?(?:[,.]\d{1,3})?\\*\\?[\])]\s*",
        "",
        text,
    )
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            lines.append("")
            continue
        if TIMESTAMP_RE.match(stripped):
            continue
        lines.append(line)
    return "\n".join(lines)


class _ClassTextParser(HTMLParser):
    def __init__(self, class_name: str) -> None:
        super().__init__(convert_charrefs=True)
        self.class_name = class_name
        self.capture_depth = 0
        self.skip_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_lower = tag.lower()
        if self.capture_depth and tag_lower in {"script", "style", "noscript"}:
            self.skip_depth += 1
            return
        attr_map = {key.lower(): value or "" for key, value in attrs}
        classes = set(attr_map.get("class", "").split())
        if self.capture_depth:
            self.capture_depth += 1
            if tag_lower in {"p", "div", "br", "li", "h2", "h3", "h4"}:
                self.parts.append("\n")
        elif self.class_name in classes:
            self.capture_depth = 1

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if self.skip_depth:
            if tag_lower in {"script", "style", "noscript"}:
                self.skip_depth -= 1
            return
        if self.capture_depth:
            if tag_lower in {"p", "div", "li", "h2", "h3", "h4"}:
                self.parts.append("\n")
            self.capture_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.capture_depth and not self.skip_depth:
            text = data.strip()
            if text:
                self.parts.append(text)


def _html_class_text(body: str, class_name: str) -> str:
    parser = _ClassTextParser(class_name)
    parser.feed(body)
    parser.close()
    return "\n".join(parser.parts)


class _IdSubtreeTextParser(HTMLParser):
    def __init__(self, element_id: str) -> None:
        super().__init__(convert_charrefs=True)
        self.element_id = element_id
        self.capture_depth = 0
        self.skip_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_lower = tag.lower()
        attr_map = {key.lower(): value or "" for key, value in attrs}
        if self.capture_depth:
            if tag_lower in {"script", "style", "noscript"}:
                self.skip_depth += 1
                return
            self.capture_depth += 1
            if tag_lower in {"p", "div", "br", "li", "h1", "h2", "h3", "h4", "h5"}:
                self.parts.append("\n")
        elif attr_map.get("id") == self.element_id:
            self.capture_depth = 1
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if self.skip_depth:
            if tag_lower in {"script", "style", "noscript"}:
                self.skip_depth -= 1
            return
        if self.capture_depth:
            if tag_lower in {"p", "div", "li", "h1", "h2", "h3", "h4", "h5"}:
                self.parts.append("\n")
            self.capture_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.capture_depth and not self.skip_depth:
            text = data.strip()
            if text:
                self.parts.append(text)


def _html_id_text(body: str, element_id: str) -> str:
    parser = _IdSubtreeTextParser(element_id)
    parser.feed(body)
    parser.close()
    return "\n".join(parser.parts)


class _AfterIdTextParser(HTMLParser):
    def __init__(self, start_id: str, stop_classes: set[str]) -> None:
        super().__init__(convert_charrefs=True)
        self.start_id = start_id
        self.stop_classes = stop_classes
        self.capture = False
        self.stopped = False
        self.skip_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.stopped:
            return
        tag_lower = tag.lower()
        attr_map = {key.lower(): value or "" for key, value in attrs}
        classes = set(attr_map.get("class", "").split())
        if self.capture and classes.intersection(self.stop_classes):
            self.stopped = True
            return
        if self.capture and tag_lower in {"script", "style", "noscript"}:
            self.skip_depth += 1
            return
        if attr_map.get("id") == self.start_id:
            self.capture = True
            self.parts.append("\n")
        if self.capture and tag_lower in {"p", "div", "br", "li", "h1", "h2", "h3", "h4", "hr"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self.stopped:
            return
        tag_lower = tag.lower()
        if self.skip_depth:
            if tag_lower in {"script", "style", "noscript"}:
                self.skip_depth -= 1
            return
        if self.capture and tag_lower in {"p", "div", "li", "h1", "h2", "h3", "h4", "hr"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self.capture and not self.stopped and not self.skip_depth:
            text = data.strip()
            if text:
                self.parts.append(text)


def _html_after_id_text(body: str, start_id: str, *, stop_classes: set[str] | None = None) -> str:
    parser = _AfterIdTextParser(start_id, stop_classes or set())
    parser.feed(body)
    parser.close()
    return "\n".join(parser.parts)


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


def _looks_like_inline_timestamped_transcript(kind: str, body: str) -> bool:
    sample = body[:100_000]
    return len(INLINE_TIMESTAMP_RE.findall(sample)) >= 20 or len(SHORT_OR_INLINE_TIMESTAMP_RE.findall(sample)) >= 20


def _strip_inline_timestamps(body: str) -> str:
    text = re.sub(
        r"(?m)^\s*(?:\\?[\[(][^\]\)]{0,80}\\?[\])]\s*)?\d{1,2}:\d{2}(?::\d{2})?(?:[,.]\d{1,3})?\s*[-–—]?\s*",
        "",
        body,
    )
    text = re.sub(
        r"(?m)^(\s*(?:[A-Z][A-Za-z0-9 .'-]{1,60}:)\s*)\d{1,2}:\d{2}(?::\d{2})?(?:[,.]\d{1,3})?\s*",
        r"\1",
        text,
    )
    text = re.sub(
        r"\\?[\[(][^\]\)]{0,80}?\\*\d{1,2}:\d{2}(?::\d{2})?(?:[,.]\d{1,3})?\\*\\?[\])]\s*",
        "",
        text,
    )
    text = re.sub(
        r"(?m)(^|\n|\s)\d{1,2}:\d{2}:\d{2}(?:[,.]\d{1,3})?\s*[-–—]\s*",
        r"\1",
        text,
    )
    text = re.sub(
        r"(?m)(^|\n|\s)\d{1,2}:\d{2}(?::\d{2})?(?:[,.]\d{1,3})?\s*[-–—]?\s*",
        r"\1",
        text,
    )
    return text


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
    pdf_object_markers = len(PDF_OBJECT_RE.findall(text[:100_000]))
    pdf_stream_markers = len(re.findall(r"\b(?:stream|endstream|xref|trailer|startxref)\b", text[:100_000], re.I))
    issues: list[str] = []

    if stripped.startswith(("[", "{")) and json_timing_markers >= 3:
        issues.append("raw_json_timing_payload")
    if stripped.startswith("%PDF") or pdf_object_markers >= 5:
        issues.append("raw_pdf_payload")
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
    if _is_stackoverflow_blog_shell(text, source_url=source_url, word_count=word_count):
        issues.append("official_page_boilerplate_shell")

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
        "pdf_object_markers": pdf_object_markers,
        "pdf_stream_markers": pdf_stream_markers,
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
            "raw_pdf_payload",
            "official_page_boilerplate_shell",
        }
        for issue in issues
    )
    return TranscriptQuality(ok=not issues, quarantine=quarantine, issues=issues, metrics=metrics)


def _is_stackoverflow_blog_shell(text: str, *, source_url: str | None, word_count: int) -> bool:
    if "stackoverflow.blog/" not in (source_url or "").lower():
        return False
    lowered = text[:6000].lower()
    if word_count >= 700:
        return False
    return "stack overflow blog" in lowered and "loading" in lowered


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
