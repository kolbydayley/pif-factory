"""Deterministic private gold-benchmark construction for Signal Desk.

The builder reads the canonical factory database and local transcript files,
but performs no network access and no production writes.  Split manifests are
content-addressed and contain transcript locations plus window offsets, not
transcript text.  Private Gold A/B/C packets materialize the corresponding
text only when an authoring run is prepared.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .signal_desk_intelligence import normalize_feed_url
from .signal_desk_rebuild_contracts import SCHEMA_VERSION as GLM_CONTRACT_VERSION
from .signal_desk_rebuild_contracts import event_schema


MANIFEST_SCHEMA_VERSION = "pif_signal_desk_rebuild_split_manifest_v1"
GOLD_PACKET_SCHEMA_VERSION = "pif_signal_desk_rebuild_gold_packet_v1"
DEFAULT_SEED = "signal-desk-clean-corpus-2026-08-31-v1"
DEFAULT_WINDOW_CHARS = 6_000
WINDOWS_PER_EPISODE = 3
IN_DOMAIN_EPISODES_PER_SHOW = 4
OOD_EPISODES_PER_SHOW = 4
OOD_SEALED_SHOW_COUNT = 4
OOD_SHAPES = frozenset({"claim_dense", "narrative", "structural"})
GOLD_PASSES = frozenset({"A", "B", "C"})


class SignalDeskGoldError(RuntimeError):
    """A benchmark invariant failed; no partial manifest should be used."""


GLM_OUTPUT_SCHEMA: Mapping[str, Any] = event_schema()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _stable_rank(seed: str, *parts: object) -> str:
    return _sha("|".join([seed, *(str(part) for part in parts)]))


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _resolve_local_path(value: str, project_root: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def _recorded_path(path: Path, project_root: Path) -> str:
    try:
        return str(path.relative_to(project_root.resolve()))
    except ValueError:
        return str(path)


def _verified_text(path_value: str, expected_sha: str | None, project_root: Path) -> tuple[Path, str, str]:
    path = _resolve_local_path(path_value, project_root)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise SignalDeskGoldError(f"unreadable local transcript: {path}") from exc
    observed = _sha(text)
    if expected_sha and observed != expected_sha:
        raise SignalDeskGoldError(f"transcript hash mismatch: {path}")
    if not text.strip():
        raise SignalDeskGoldError(f"empty local transcript: {path}")
    return path, text, observed


_SPEAKER_LINE = re.compile(r"(?m)^(?:\[[^\]\n]{1,80}\]|[A-Za-z][^:\n]{0,79}):\s*\S")
_PARAGRAPH_BOUNDARY = re.compile(r"\n\s*\n")
_SENTENCE_BOUNDARY = re.compile(r"[.!?][\"'\)\]]?\s+")


def _sentence_spans(text: str, *, offset: int = 0) -> list[tuple[int, int]]:
    """Find sentence-like spans in one linear boundary scan."""

    spans: list[tuple[int, int]] = []
    start = 0
    for match in _SENTENCE_BOUNDARY.finditer(text):
        end = match.end()
        if text[start:end].strip():
            spans.append((offset + start, offset + end))
        start = end
    if text[start:].strip():
        spans.append((offset + start, offset + len(text)))
    return spans


def _paragraph_spans(text: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    start = 0
    for match in _PARAGRAPH_BOUNDARY.finditer(text):
        end = match.start()
        if text[start:end].strip():
            spans.append((start, end))
        start = match.end()
    if text[start:].strip():
        spans.append((start, len(text)))
    return spans


def _hard_split_span(text: str, start: int, end: int, limit: int) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    cursor = start
    while end - cursor > limit:
        cut = cursor + limit
        whitespace = text.rfind(" ", cursor + max(1, limit // 2), cut + 1)
        if whitespace > cursor:
            cut = whitespace
        spans.append((cursor, cut))
        cursor = cut
        while cursor < end and text[cursor].isspace():
            cursor += 1
    if cursor < end:
        spans.append((cursor, end))
    return spans


def _turn_spans(text: str, limit: int) -> tuple[list[tuple[int, int]], str]:
    speaker_starts = [match.start() for match in _SPEAKER_LINE.finditer(text)]
    if len(speaker_starts) >= 3:
        starts = ([0] if text[: speaker_starts[0]].strip() else []) + speaker_starts
        spans = [
            (start, starts[index + 1] if index + 1 < len(starts) else len(text))
            for index, start in enumerate(starts)
        ]
        alignment = "speaker_turn"
    else:
        spans = _paragraph_spans(text)
        if len(spans) < 3:
            spans = _sentence_spans(text)
        alignment = "sentence_fallback"
    expanded: list[tuple[int, int]] = []
    for start, end in spans:
        if end - start <= limit:
            expanded.append((start, end))
            continue
        sentences = _sentence_spans(text[start:end], offset=start)
        if len(sentences) > 1 and max(b - a for a, b in sentences) <= limit:
            expanded.extend(sentences)
        else:
            expanded.extend(_hard_split_span(text, start, end, limit))
    return [(a, b) for a, b in expanded if text[a:b].strip()], alignment


def turn_aligned_windows(
    text: str,
    *,
    max_chars: int = DEFAULT_WINDOW_CHARS,
    count: int = WINDOWS_PER_EPISODE,
) -> tuple[dict[str, Any], ...]:
    """Return exactly ``count`` deterministic, contiguous transcript windows."""

    if max_chars < 256 or count < 1:
        raise SignalDeskGoldError("invalid window contract")
    units, alignment = _turn_spans(text, max_chars)
    if len(units) < count:
        raise SignalDeskGoldError("transcript cannot produce three aligned windows")
    capacity = min(max_chars, max(256, math.ceil(len(text) / count)))
    anchors = [((2 * index + 1) * len(text)) / (2 * count) for index in range(count)]
    results: list[dict[str, Any]] = []
    for anchor in anchors:
        center = min(
            range(len(units)),
            key=lambda index: (abs(((units[index][0] + units[index][1]) / 2) - anchor), index),
        )
        left = right = center
        while True:
            options: list[tuple[float, int, int]] = []
            if left > 0 and units[right][1] - units[left - 1][0] <= capacity:
                options.append((abs(((units[left - 1][0] + units[right][1]) / 2) - anchor), left - 1, right))
            if right + 1 < len(units) and units[right + 1][1] - units[left][0] <= capacity:
                options.append((abs(((units[left][0] + units[right + 1][1]) / 2) - anchor), left, right + 1))
            if not options:
                break
            _, left, right = min(options)
        start, end = units[left][0], units[right][1]
        window_text = text[start:end]
        results.append(
            {
                "start_char": start,
                "end_char": end,
                "char_count": len(window_text),
                "text_sha256": _sha(window_text),
                "alignment": alignment,
            }
        )
    if len({row["text_sha256"] for row in results}) != count:
        raise SignalDeskGoldError("transcript produced duplicate benchmark windows")
    if any(int(row["char_count"]) > max_chars for row in results):
        raise SignalDeskGoldError("window exceeds frozen character contract")
    return tuple(results)


def _canonical_transcript_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    source_columns = _table_columns(conn, "sources")
    episode_columns = _table_columns(conn, "episodes")
    transcript_columns = _table_columns(conn, "transcripts")
    prep_columns = _table_columns(conn, "transcript_preparations")
    required = {
        "sources": {"id", "name", "rss_url"},
        "episodes": {"id", "source_id", "guid", "title"},
        "transcripts": {"id", "episode_id", "raw_text_path", "raw_text_sha256", "status"},
    }
    observed = {
        "sources": source_columns,
        "episodes": episode_columns,
        "transcripts": transcript_columns,
    }
    for table, columns in required.items():
        if not columns <= observed[table]:
            raise SignalDeskGoldError(f"factory schema missing required {table} columns")
    if not {"transcript_id", "cleaned_text_path"} <= prep_columns:
        raise SignalDeskGoldError("factory schema missing transcript preparation columns")
    enabled = "AND s.enabled = 1" if "enabled" in source_columns else ""
    published = "e.published_at" if "published_at" in episode_columns else "NULL"
    prep_sha = "p.cleaned_text_sha256" if "cleaned_text_sha256" in prep_columns else "NULL"
    quality = "p.quality_score" if "quality_score" in prep_columns else "0"
    prep_status = "p.status" if "status" in prep_columns else "NULL"
    cursor = conn.execute(
        f"""
        SELECT s.id source_id, s.name show_name, s.rss_url,
               e.id episode_id, e.guid episode_guid, e.title episode_title, {published} published_at,
               t.id transcript_id, t.raw_text_path, t.raw_text_sha256,
               p.cleaned_text_path, {prep_sha} cleaned_text_sha256,
               {quality} preparation_quality, {prep_status} preparation_status
          FROM sources s
          JOIN episodes e ON e.source_id = s.id
          JOIN transcripts t ON t.episode_id = e.id AND t.status = 'ready'
          LEFT JOIN transcript_preparations p ON p.transcript_id = t.id
         WHERE s.rss_url IS NOT NULL AND TRIM(s.rss_url) <> '' {enabled}
         ORDER BY s.id, e.id, t.id
        """
    )
    column_names = [str(item[0]) for item in cursor.description or ()]
    rows = cursor.fetchall()
    by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw) if isinstance(raw, sqlite3.Row) else dict(zip(column_names, raw))
        row["use_prepared"] = bool(
            row.get("cleaned_text_path")
            and str(row.get("preparation_status") or "").casefold() in {"prepared", "completed", "ready"}
        )
        by_episode[str(row["episode_id"])].append(row)
    selected: list[dict[str, Any]] = []
    for episode_id, candidates in sorted(by_episode.items()):
        candidates.sort(
            key=lambda row: (
                not row["use_prepared"],
                -float(row.get("preparation_quality") or 0),
                str(row["transcript_id"]),
            )
        )
        selected.append(candidates[0])
    return selected


def _window_records(
    *,
    corpus: str,
    split: str,
    show_id: str,
    show_name: str,
    canonical_feed: str,
    source_shape: str | None,
    episode_id: str,
    episode_title: str,
    transcript_id: str,
    transcript_path: Path,
    transcript_sha: str,
    text: str,
    project_root: Path,
    max_chars: int,
) -> list[dict[str, Any]]:
    windows = turn_aligned_windows(text, max_chars=max_chars)
    records = []
    for index, window in enumerate(windows):
        identity = f"{corpus}|{show_id}|{episode_id}|{index}|{window['text_sha256']}"
        records.append(
            {
                "window_id": f"sdw_{_sha(identity)[:20]}",
                "corpus": corpus,
                "split": split,
                "show_id": show_id,
                "show_name": show_name,
                "canonical_feed": canonical_feed,
                "source_shape": source_shape,
                "episode_id": episode_id,
                "episode_title": episode_title,
                "transcript_id": transcript_id,
                "transcript_path": _recorded_path(transcript_path, project_root),
                "transcript_sha256": transcript_sha,
                "window_index": index,
                **window,
            }
        )
    return records


def _validate_unique_feeds(
    rows: Sequence[Mapping[str, Any]],
    show_aliases: Mapping[str, str] | None = None,
) -> dict[str, list[Mapping[str, Any]]]:
    by_feed: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    feed_sources: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        feed = normalize_feed_url(str(row.get("rss_url") or ""))
        if not feed:
            continue
        by_feed[feed].append(row)
        source_id = str(row["source_id"])
        feed_sources[feed].add(str((show_aliases or {}).get(source_id, source_id)))
    duplicates = {feed: ids for feed, ids in feed_sources.items() if len(ids) > 1}
    if duplicates:
        detail = "; ".join(f"{feed}: {sorted(ids)}" for feed, ids in sorted(duplicates.items()))
        raise SignalDeskGoldError(f"duplicate canonical RSS feeds require adjudication: {detail}")
    return by_feed


def _build_in_domain(
    conn: sqlite3.Connection,
    *,
    project_root: Path,
    seed: str,
    max_chars: int,
    show_aliases: Mapping[str, str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = _canonical_transcript_rows(conn)
    by_feed = _validate_unique_feeds(rows, show_aliases)
    windows: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    for feed, candidates in sorted(by_feed.items()):
        eligible: list[tuple[dict[str, Any], Path, str, str]] = []
        canonical_ids = {
            str((show_aliases or {}).get(str(row["source_id"]), str(row["source_id"])))
            for row in candidates
        }
        if len(canonical_ids) != 1:
            raise SignalDeskGoldError(f"feed alias registry is ambiguous: {feed}")
        source_id = next(iter(canonical_ids))
        canonical_rows = [row for row in candidates if str(row["source_id"]) == source_id]
        show_name = str((canonical_rows or candidates)[0]["show_name"])
        candidate_by_guid: dict[str, Mapping[str, Any]] = {}
        for row in candidates:
            guid = str(row.get("episode_guid") or row["episode_id"])
            existing = candidate_by_guid.get(guid)
            if existing is None or (
                str(row["source_id"]) == source_id
                and str(existing["source_id"]) != source_id
            ):
                candidate_by_guid[guid] = row
        ranked_candidates = sorted(
            candidate_by_guid.values(),
            key=lambda row: (
                _stable_rank(seed, feed, row["episode_id"]),
                str(row["episode_id"]),
            ),
        )
        for row in ranked_candidates:
            path_value = row["cleaned_text_path"] if row["use_prepared"] else row["raw_text_path"]
            expected = row["cleaned_text_sha256"] if row["use_prepared"] else row["raw_text_sha256"]
            try:
                path, text, digest = _verified_text(str(path_value), str(expected or ""), project_root)
                turn_aligned_windows(text, max_chars=max_chars)
            except SignalDeskGoldError:
                continue
            eligible.append((row, path, text, digest))
            if len(eligible) >= IN_DOMAIN_EPISODES_PER_SHOW:
                break
        if len(eligible) < IN_DOMAIN_EPISODES_PER_SHOW:
            continue
        chosen = eligible[:IN_DOMAIN_EPISODES_PER_SHOW]
        assignments = ("development", "validation", "validation", "sealed_holdout")
        show_episode_ids = []
        for (row, path, text, digest), split in zip(chosen, assignments):
            episode_id = str(row["episode_id"])
            show_episode_ids.append(episode_id)
            windows.extend(
                _window_records(
                    corpus="in_domain",
                    split=split,
                    show_id=source_id,
                    show_name=show_name,
                    canonical_feed=feed,
                    source_shape=None,
                    episode_id=episode_id,
                    episode_title=str(row["episode_title"]),
                    transcript_id=str(row["transcript_id"]),
                    transcript_path=path,
                    transcript_sha=digest,
                    text=text,
                    project_root=project_root,
                    max_chars=max_chars,
                )
            )
        shows.append(
            {
                "show_id": source_id,
                "show_name": show_name,
                "canonical_feed": feed,
                "corpus": "in_domain",
                "episode_ids": show_episode_ids,
            }
        )
    return shows, windows


def _build_ood(
    entries: Sequence[Mapping[str, Any]],
    *,
    project_root: Path,
    seed: str,
    max_chars: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not entries:
        return [], []
    if len({str(entry.get("show_id") or "") for entry in entries}) != len(entries):
        raise SignalDeskGoldError("OOD show_id values must be nonempty and unique")
    sealed_count = sum(bool(entry.get("sealed")) for entry in entries)
    if sealed_count != OOD_SEALED_SHOW_COUNT:
        raise SignalDeskGoldError("exactly four whole OOD shows must be sealed")
    shows: list[dict[str, Any]] = []
    windows: list[dict[str, Any]] = []
    seen_paths: set[tuple[str, str]] = set()
    for entry in sorted(entries, key=lambda row: str(row["show_id"])):
        show_id = str(entry["show_id"])
        show_name = str(entry.get("show_name") or "").strip()
        shape = str(entry.get("source_shape") or "")
        if not show_name or shape not in OOD_SHAPES:
            raise SignalDeskGoldError(f"invalid OOD show metadata: {show_id}")
        episodes = list(entry.get("episodes") or [])
        if len(episodes) < OOD_EPISODES_PER_SHOW:
            raise SignalDeskGoldError(f"OOD show requires four local episodes: {show_id}")
        episodes.sort(key=lambda row: (_stable_rank(seed, "ood", show_id, row.get("episode_id")), str(row.get("episode_id"))))
        chosen = episodes[:OOD_EPISODES_PER_SHOW]
        sealed = bool(entry.get("sealed"))
        assignments = ("sealed_holdout",) * 4 if sealed else ("development", "validation", "validation", "validation")
        episode_ids = []
        for episode, split in zip(chosen, assignments):
            episode_id = str(episode.get("episode_id") or "")
            transcript_id = str(episode.get("transcript_id") or episode_id)
            if not episode_id or not episode.get("transcript_path"):
                raise SignalDeskGoldError(f"invalid OOD episode metadata: {show_id}")
            path, text, digest = _verified_text(
                str(episode["transcript_path"]),
                str(episode.get("transcript_sha256") or ""),
                project_root,
            )
            path_key = (episode_id, str(path))
            if path_key in seen_paths:
                raise SignalDeskGoldError("duplicate OOD episode/transcript entry")
            seen_paths.add(path_key)
            episode_ids.append(episode_id)
            windows.extend(
                _window_records(
                    corpus="ood",
                    split=split,
                    show_id=show_id,
                    show_name=show_name,
                    canonical_feed=normalize_feed_url(str(entry.get("rss_url") or "")),
                    source_shape=shape,
                    episode_id=episode_id,
                    episode_title=str(episode.get("episode_title") or episode_id),
                    transcript_id=transcript_id,
                    transcript_path=path,
                    transcript_sha=digest,
                    text=text,
                    project_root=project_root,
                    max_chars=max_chars,
                )
            )
        shows.append(
            {
                "show_id": show_id,
                "show_name": show_name,
                "canonical_feed": normalize_feed_url(str(entry.get("rss_url") or "")),
                "corpus": "ood",
                "source_shape": shape,
                "sealed": sealed,
                "episode_ids": episode_ids,
            }
        )
    return shows, windows


def validate_split_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise SignalDeskGoldError("unsupported split manifest schema")
    windows = list(manifest.get("windows") or [])
    if not windows:
        raise SignalDeskGoldError("benchmark has no eligible windows")
    ids = [str(row.get("window_id") or "") for row in windows]
    if not all(ids) or len(ids) != len(set(ids)):
        raise SignalDeskGoldError("window IDs must be unique and nonempty")
    episode_splits: dict[str, set[str]] = defaultdict(set)
    episode_counts: dict[str, int] = defaultdict(int)
    for row in windows:
        episode = str(row.get("episode_id") or "")
        episode_splits[episode].add(str(row.get("split") or ""))
        episode_counts[episode] += 1
        if int(row.get("char_count") or 0) > int(manifest["window_contract"]["max_chars"]):
            raise SignalDeskGoldError("manifest contains an oversized window")
    if any(len(splits) != 1 for splits in episode_splits.values()):
        raise SignalDeskGoldError("episode leakage across splits")
    if any(count != WINDOWS_PER_EPISODE for count in episode_counts.values()):
        raise SignalDeskGoldError("each selected episode must contribute three windows")
    ood_by_show: dict[str, set[str]] = defaultdict(set)
    for row in windows:
        if row.get("corpus") == "ood":
            ood_by_show[str(row["show_id"])].add(str(row["split"]))
    for show in manifest.get("shows") or []:
        if show.get("corpus") == "ood" and show.get("sealed"):
            if ood_by_show[str(show["show_id"])] != {"sealed_holdout"}:
                raise SignalDeskGoldError("sealed OOD show leaked into a visible split")


def build_split_manifest(
    conn: sqlite3.Connection,
    *,
    project_root: Path,
    ood_entries: Sequence[Mapping[str, Any]] = (),
    seed: str = DEFAULT_SEED,
    max_chars: int = DEFAULT_WINDOW_CHARS,
    expected_in_domain_shows: int | None = None,
    expected_ood_shows: int | None = None,
    show_aliases: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build and hash-freeze a deterministic split manifest."""

    in_shows, in_windows = _build_in_domain(
        conn,
        project_root=project_root,
        seed=seed,
        max_chars=max_chars,
        show_aliases=show_aliases,
    )
    ood_shows, ood_windows = _build_ood(
        ood_entries, project_root=project_root, seed=seed, max_chars=max_chars
    )
    if expected_in_domain_shows is not None and len(in_shows) != expected_in_domain_shows:
        raise SignalDeskGoldError(
            f"expected {expected_in_domain_shows} canonical in-domain shows, found {len(in_shows)}"
        )
    if expected_ood_shows is not None and len(ood_shows) != expected_ood_shows:
        raise SignalDeskGoldError(
            f"expected {expected_ood_shows} OOD shows, found {len(ood_shows)}"
        )
    windows = sorted(in_windows + ood_windows, key=lambda row: str(row["window_id"]))
    shows = sorted(in_shows + ood_shows, key=lambda row: (str(row["corpus"]), str(row["show_id"])))
    counts: dict[str, int] = defaultdict(int)
    for row in windows:
        counts[str(row["split"])] += 1
    payload: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "seed": seed,
        "frozen": True,
        "window_contract": {
            "max_chars": max_chars,
            "windows_per_episode": WINDOWS_PER_EPISODE,
            "representation": "turn_aligned_contiguous_private_text",
            "glm_contract_version": GLM_CONTRACT_VERSION,
        },
        "counts": {
            "shows": len(shows),
            "episodes": len({row["episode_id"] for row in windows}),
            "windows": len(windows),
            "by_split": dict(sorted(counts.items())),
        },
        "shows": shows,
        "show_aliases": dict(sorted((show_aliases or {}).items())),
        "windows": windows,
    }
    validate_split_manifest(payload)
    payload["manifest_sha256"] = _sha(_canonical_json(payload))
    return payload


def verify_frozen_manifest(manifest: Mapping[str, Any]) -> None:
    validate_split_manifest(manifest)
    expected = str(manifest.get("manifest_sha256") or "")
    unhashed = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if not expected or _sha(_canonical_json(unhashed)) != expected:
        raise SignalDeskGoldError("split manifest hash verification failed")


def _packet_for_window(
    window: Mapping[str, Any],
    *,
    project_root: Path,
    gold_pass: str,
) -> dict[str, Any]:
    path, text, digest = _verified_text(
        str(window["transcript_path"]), str(window["transcript_sha256"]), project_root
    )
    del path
    if digest != window["transcript_sha256"]:
        raise SignalDeskGoldError("transcript changed after split freeze")
    start, end = int(window["start_char"]), int(window["end_char"])
    window_text = text[start:end]
    if _sha(window_text) != window["text_sha256"]:
        raise SignalDeskGoldError("window changed after split freeze")
    packet_identity = f"{gold_pass}|{window['window_id']}"
    return {
        "schema_version": GOLD_PACKET_SCHEMA_VERSION,
        "contract_version": GLM_CONTRACT_VERSION,
        "packet_id": f"sdp_{_sha(packet_identity)[:20]}",
        "gold_pass": gold_pass,
        "input": {
            "window_id": window["window_id"],
            "show_id": window["show_id"],
            "episode_id": window["episode_id"],
            "split": window["split"],
            "window_text": window_text,
        },
        "output_schema": GLM_OUTPUT_SCHEMA,
        "privacy": "private_local_transcript_text_never_public_payload",
    }


def build_gold_packets(
    manifest: Mapping[str, Any],
    *,
    project_root: Path,
    gold_pass: str,
    splits: Iterable[str] = ("development",),
    allow_sealed: bool = False,
) -> tuple[dict[str, Any], ...]:
    """Materialize private Gold A/B/C packets under the exact GLM contract."""

    verify_frozen_manifest(manifest)
    normalized_pass = gold_pass.upper()
    if normalized_pass not in GOLD_PASSES:
        raise SignalDeskGoldError("gold_pass must be A, B, or C")
    selected_splits = frozenset(str(value) for value in splits)
    if "sealed_holdout" in selected_splits and not allow_sealed:
        raise SignalDeskGoldError("sealed holdout requires explicit authorization")
    packets = [
        _packet_for_window(row, project_root=project_root, gold_pass=normalized_pass)
        for row in manifest["windows"]
        if row["split"] in selected_splits
    ]
    return tuple(sorted(packets, key=lambda row: str(row["input"]["window_id"])))


def _stratified_window_order(windows: Sequence[Mapping[str, Any]], seed: str) -> list[str]:
    strata: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)
    for row in windows:
        key = (
            str(row.get("corpus") or ""),
            str(row.get("source_shape") or "none"),
            str(row.get("split") or ""),
            str(row.get("show_id") or ""),
        )
        strata[key].append(str(row["window_id"]))
    queues: list[deque[str]] = []
    for key, values in sorted(strata.items()):
        values.sort(key=lambda value: (_stable_rank(seed, "audit", *key, value), value))
        queues.append(deque(values))
    order: list[str] = []
    while queues:
        remaining = []
        for queue in queues:
            if queue:
                order.append(queue.popleft())
            if queue:
                remaining.append(queue)
        queues = remaining
    return order


def select_gold_audit(
    manifest: Mapping[str, Any],
    event_counts: Mapping[str, int],
    *,
    seed: str = DEFAULT_SEED,
    initial_windows: int = 120,
    expansion_block: int = 40,
    minimum_events: int = 1_000,
) -> dict[str, Any]:
    """Select a deterministic stratified audit, expanding for event power."""

    verify_frozen_manifest(manifest)
    if initial_windows < 1 or expansion_block < 1 or minimum_events < 1:
        raise SignalDeskGoldError("invalid audit sampling contract")
    window_ids = {str(row["window_id"]) for row in manifest["windows"]}
    missing = sorted(window_ids - set(event_counts))
    if missing:
        raise SignalDeskGoldError(f"event counts missing for {len(missing)} benchmark windows")
    if any(int(event_counts[window_id]) < 0 for window_id in window_ids):
        raise SignalDeskGoldError("gold event counts must be nonnegative")
    order = _stratified_window_order(manifest["windows"], seed)
    sample_size = min(initial_windows, len(order))
    total_events = sum(int(event_counts[window_id]) for window_id in order[:sample_size])
    while total_events < minimum_events and sample_size < len(order):
        sample_size = min(len(order), sample_size + expansion_block)
        total_events = sum(int(event_counts[window_id]) for window_id in order[:sample_size])
    selected = order[:sample_size]
    result = {
        "schema_version": "pif_signal_desk_rebuild_gold_audit_sample_v1",
        "seed": seed,
        "manifest_sha256": manifest["manifest_sha256"],
        "initial_windows": initial_windows,
        "expansion_block": expansion_block,
        "minimum_events": minimum_events,
        "sample_window_ids": selected,
        "sample_windows": len(selected),
        "sample_events": total_events,
        "expanded": len(selected) > min(initial_windows, len(order)),
        "exhausted": len(selected) == len(order) and total_events < minimum_events,
        "decision_ready": total_events >= minimum_events,
    }
    result["sample_sha256"] = _sha(_canonical_json(result))
    return result
