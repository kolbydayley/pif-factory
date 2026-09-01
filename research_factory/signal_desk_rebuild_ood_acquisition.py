"""Sealed-safe acquisition of official OOD transcript fixtures.

Only explicit first-party URLs are accepted. Transcript text is written to the
physically separate benchmark tree and is never printed or placed in receipts.
"""

from __future__ import annotations

import argparse
from io import BytesIO
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from pypdf import PdfReader

logging.getLogger("pypdf").setLevel(logging.ERROR)

from .signal_desk_rebuild_benchmark import (
    BENCHMARK_SOURCE_NAMESPACE,
    initialize_benchmark_store,
    put_benchmark_transcript,
)
from .signal_desk_rebuild_gold import turn_aligned_windows
from .util import now_iso, write_text_atomic


SCHEMA_VERSION = "pif_signal_desk_rebuild_ood_acquisition_v1"
ALLOWED_SHAPES = frozenset({"claim_dense", "narrative", "format_stress"})
MIN_TRANSCRIPT_WORDS = 800
MIN_EPISODES = 4
MAX_EPISODES = 6
USER_AGENT = "Mozilla/5.0 (Signal Desk private benchmark acquisition)"


class OodAcquisitionError(RuntimeError):
    pass


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _word_count(value: str) -> int:
    return len(re.findall(r"\b[\w'’-]+\b", value))


def _published_at(soup: BeautifulSoup) -> str:
    # Several first-party archives were migrated in bulk and expose the
    # migration timestamp as article:published_time. Prefer the episode/story
    # air date printed by the publisher when present so time-stratified
    # sampling reflects the source material rather than a CMS migration.
    visible = " ".join(soup.get_text(" ", strip=True).split())
    air_date = re.search(
        r"(?:Original Air Date|originally aired(?: on)?)\s*"
        r"([A-Z][a-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})",
        visible,
        flags=re.IGNORECASE,
    )
    if air_date:
        normalized = re.sub(r"(\d)(?:st|nd|rd|th)", r"\1", air_date.group(1))
        for fmt in ("%B %d, %Y", "%B %d %Y"):
            try:
                return datetime.strptime(normalized, fmt).replace(
                    tzinfo=timezone.utc
                ).isoformat()
            except ValueError:
                continue
    transcript_date = re.search(
        r"Transcript.{0,100}?([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        visible,
        flags=re.IGNORECASE,
    )
    if transcript_date:
        try:
            return datetime.strptime(transcript_date.group(1), "%B %d, %Y").replace(
                tzinfo=timezone.utc
            ).isoformat()
        except ValueError:
            pass
    candidates: list[str] = []
    for key, value in (
        ("property", "article:published_time"),
        ("name", "date"),
        ("name", "pubdate"),
        ("itemprop", "datePublished"),
    ):
        node = soup.find("meta", attrs={key: value})
        if node and node.get("content"):
            candidates.append(str(node["content"]))
    for node in soup.find_all("time"):
        if node.get("datetime"):
            candidates.append(str(node["datetime"]))
    for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            payload = json.loads(node.string or "")
        except json.JSONDecodeError:
            continue
        stack = [payload]
        while stack:
            value = stack.pop()
            if isinstance(value, Mapping):
                if value.get("datePublished"):
                    candidates.append(str(value["datePublished"]))
                stack.extend(value.values())
            elif isinstance(value, list):
                stack.extend(value)
    for raw in candidates:
        try:
            parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            continue
    raise OodAcquisitionError("official page omitted a machine-readable publication date")


def _transcript_container(soup: BeautifulSoup, url: str):
    markers = []
    for node in soup.find_all(string=True):
        normalized = " ".join(str(node).split()).casefold()
        if normalized in {"transcript", "episode transcript", "full transcript"}:
            markers.append(node.parent)
    candidates = []
    for marker in markers:
        for ancestor in [marker, *marker.parents]:
            if ancestor.name in {"body", "html", "[document]"}:
                break
            words = _word_count(ancestor.get_text(" ", strip=True))
            if words >= MIN_TRANSCRIPT_WORDS:
                candidates.append((words, ancestor))
                break
    if not candidates and "/transcript" in urlparse(url).path.casefold():
        for selector in ("article", "main", ".entry-content", ".sqs-block-content"):
            for node in soup.select(selector):
                words = _word_count(node.get_text(" ", strip=True))
                if words >= MIN_TRANSCRIPT_WORDS:
                    candidates.append((words, node))
    if not candidates:
        raise OodAcquisitionError("official page did not expose a transcript container")
    return min(candidates, key=lambda item: item[0])[1]


def extract_official_page(html: bytes, *, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    published_at = _published_at(soup)
    title_node = soup.find("meta", attrs={"property": "og:title"})
    if not title_node:
        title_node = next(
            (node for node in soup.find_all("h1") if node.get_text(" ", strip=True)),
            None,
        )
    if not title_node:
        raise OodAcquisitionError("official page omitted an h1 episode title")
    title = " ".join(
        (
            str(title_node.get("content"))
            if title_node.name == "meta"
            else title_node.get_text(" ", strip=True)
        ).split()
    )
    for node in soup.select("script,style,noscript,nav,header,footer,form,aside"):
        node.decompose()
    container = _transcript_container(soup, url)
    blocks = []
    for node in container.find_all(["p", "blockquote", "h3", "h4", "li"]):
        value = " ".join(node.get_text(" ", strip=True).split())
        if value and (not blocks or blocks[-1] != value):
            blocks.append(value)
    text = "\n\n".join(blocks).strip() + "\n"
    if _word_count(text) < MIN_TRANSCRIPT_WORDS:
        # Some official transcript widgets (notably Hidden Brain) place the
        # transcript as direct text nodes inside one isolated accordion rather
        # than semantic paragraphs. The container boundary still excludes the
        # rest of the page, so preserve its line breaks without flattening the
        # entire document.
        lines = [
            " ".join(value.split())
            for value in container.get_text("\n", strip=True).splitlines()
            if value.strip()
        ]
        text = "\n\n".join(lines).strip() + "\n"
    if _word_count(text) < MIN_TRANSCRIPT_WORDS:
        raise OodAcquisitionError("parsed official transcript is implausibly thin")
    turn_aligned_windows(text)
    return {
        "title": title,
        "published_at": published_at,
        "text": text,
        "word_count": _word_count(text),
    }


def extract_official_pdf(
    pdf: bytes, *, title: str, published_at: str
) -> dict[str, Any]:
    if not pdf.startswith(b"%PDF") or not title or not published_at:
        raise OodAcquisitionError("official PDF requires explicit title and publication date")
    try:
        reader = PdfReader(BytesIO(pdf))
        pages = [str(page.extract_text() or "").strip() for page in reader.pages]
    except Exception as exc:  # pragma: no cover - parser exposes varied errors
        raise OodAcquisitionError("official transcript PDF could not be parsed") from exc
    text = "\n\n".join(page for page in pages if page).strip() + "\n"
    if _word_count(text) < MIN_TRANSCRIPT_WORDS:
        raise OodAcquisitionError("parsed official transcript PDF is implausibly thin")
    turn_aligned_windows(text)
    try:
        parsed = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OodAcquisitionError("official PDF publication date is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return {
        "title": title.strip(),
        "published_at": parsed.astimezone(timezone.utc).isoformat(),
        "text": text,
        "word_count": _word_count(text),
    }


def _fetch(url: str, *, allowed_hosts: frozenset[str]) -> bytes:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in allowed_hosts:
        raise OodAcquisitionError("OOD URL is not on the configured first-party HTTPS host")
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=45) as response:  # noqa: S310 - explicit first-party host gate
        final = urlparse(response.geturl())
        if final.scheme != "https" or final.hostname not in allowed_hosts:
            raise OodAcquisitionError("OOD URL redirected off the configured first-party host")
        return response.read()


def acquire_show(
    spec: Mapping[str, Any], *, output_root: Path, benchmark_database: Path,
    canonical_database: Path
) -> dict[str, Any]:
    show_id = str(spec.get("show_id") or "")
    show_name = str(spec.get("show_name") or "")
    shape = str(spec.get("source_shape") or "")
    sealed = bool(spec.get("sealed"))
    host = str(spec.get("first_party_host") or "")
    allowed_hosts = frozenset(
        [host, *[str(value) for value in spec.get("allowed_redirect_hosts") or ()]]
    )
    raw_entries = list(spec.get("official_urls") or ())
    entries = [
        {"url": value} if isinstance(value, str) else dict(value)
        for value in raw_entries
    ]
    urls = [str(value.get("url") or "") for value in entries]
    if not show_id or not show_name or shape not in ALLOWED_SHAPES or not host:
        raise OodAcquisitionError("invalid OOD show specification")
    if not MIN_EPISODES <= len(urls) <= MAX_EPISODES or len(set(urls)) != len(urls):
        raise OodAcquisitionError("OOD show requires four to six unique official URLs")
    show_root = output_root / ("sealed" if sealed else "visible") / show_id
    episodes = []
    conn = initialize_benchmark_store(benchmark_database, canonical_database)
    try:
        for url, entry in zip(urls, entries):
            body = _fetch(url, allowed_hosts=allowed_hosts)
            if body.startswith(b"%PDF"):
                page = extract_official_pdf(
                    body,
                    title=str(entry.get("title") or ""),
                    published_at=str(entry.get("published_at") or ""),
                )
            else:
                page = extract_official_page(body, url=url)
            episode_id = f"ood_{show_id}_{_sha_bytes(url.encode())[:16]}"
            text_path = show_root / f"{episode_id}.txt"
            # Freeze the exact same bytes in the filesystem and isolated store.
            # A trailing newline must not create two revision hashes for one
            # benchmark transcript.
            frozen_text = str(page["text"]).strip()
            write_text_atomic(text_path, frozen_text)
            digest = _sha_bytes(frozen_text.encode())
            stored = put_benchmark_transcript(
                conn,
                show_id=show_id,
                show_name=show_name,
                cohort=shape,
                episode_id=episode_id,
                title=page["title"],
                official_url=url,
                transcript_text=frozen_text,
                published_at=page["published_at"],
                source_kind="official_first_party_transcript",
            )
            episodes.append(
                {
                    "episode_id": episode_id,
                    "episode_title": page["title"],
                    "published_at": page["published_at"],
                    "transcript_path": str(text_path.resolve()),
                    "transcript_sha256": digest,
                    "word_count": page["word_count"],
                    "official_url_sha256": _sha_bytes(url.encode()),
                    "source_kind": "official_first_party_transcript",
                    "source_namespace": stored["source_namespace"],
                }
            )
        conn.commit()
    finally:
        conn.close()
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "show_id": show_id,
        "show_name": show_name,
        "source_shape": shape,
        "sealed": sealed,
        "episode_count": len(episodes),
        "episodes": episodes,
        "storage_namespace": BENCHMARK_SOURCE_NAMESPACE,
        "public_aggregation_access": False,
        "privacy": (
            "sealed_transcript_text_not_logged_or_embedded_in_receipt"
            if sealed
            else "private_benchmark_transcript_text_not_public"
        ),
    }
    receipt["receipt_sha256"] = _sha_bytes(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    )
    write_text_atomic(show_root / "receipt.json", json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def acquire_manifest(
    manifest_path: Path, *, output_root: Path, benchmark_database: Path,
    canonical_database: Path
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    receipts = [
        acquire_show(
            spec,
            output_root=output_root,
            benchmark_database=benchmark_database,
            canonical_database=canonical_database,
        )
        for spec in manifest.get("shows") or ()
    ]
    # Deliberately exclude titles, URLs, paths, or transcript-adjacent values
    # from stdout so sealed acquisition cannot leak into prompt development.
    return {
        "schema_version": "pif_signal_desk_rebuild_ood_acquisition_run_v1",
        "show_count": len(receipts),
        "episode_count": sum(int(row["episode_count"]) for row in receipts),
        "sealed_show_count": sum(bool(row["sealed"]) for row in receipts),
        "show_receipt_sha256": sorted(str(row["receipt_sha256"]) for row in receipts),
        "transcript_text_logged": False,
        "public_aggregation_access": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--benchmark-database", type=Path, required=True)
    parser.add_argument("--canonical-database", type=Path, required=True)
    args = parser.parse_args(argv)
    result = acquire_manifest(
        args.manifest.resolve(),
        output_root=args.output_root.resolve(),
        benchmark_database=args.benchmark_database.resolve(),
        canonical_database=args.canonical_database.resolve(),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
