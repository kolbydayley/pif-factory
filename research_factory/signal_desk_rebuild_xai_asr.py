"""Bounded xAI speech-to-text acquisition for blocked rebuild shows.

The lane transcribes exactly four period-spread, duration-qualified episodes
per requested show. It writes only to the ignored benchmark work tree and
emits a private, hash-frozen overlay receipt; canonical tables are read-only.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import getpass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .signal_desk_rebuild_acquisition import select_period_spread, validate_transcript_ingest
from .signal_desk_rebuild_benchmark import episode_title_tokens, unwrap_tracking_url
from .util import now_iso, write_text_atomic


RECEIPT_SCHEMA_VERSION = "pif_benchmark_acquisition_receipt_v1"
XAI_STT_ENDPOINT = "https://api.x.ai/v1/stt"
XAI_STT_RATE_USD_PER_HOUR = 0.10
EPISODES_PER_SHOW = 4
MIN_DURATION_SECONDS = 600
MAX_CONCURRENCY = 4
MAX_CONTENT_VERIFIED_DURATION_OVERAGE_SECONDS = 600
_GENERIC_TITLE_TOKENS = frozenset(
    {"a", "an", "and", "the", "of", "to", "in", "on", "for", "with", "how", "i", "built"}
)
ALLOWED_SOURCE_IDS = frozenset(
    {"how-i-built-this", "search-engine", "tech-brew-ride-home", "the-gradient"}
)


class XaiAsrError(RuntimeError):
    pass


def _content_verified_duration_overage(
    row: Mapping[str, Any], *, text: str, observed_duration: float
) -> bool:
    """Allow bounded inserted-ad overage only when the transcript identifies the episode."""
    expected = int(row["duration_seconds"])
    overage = observed_duration - expected
    title_tokens = episode_title_tokens(str(row["title"])) - _GENERIC_TITLE_TOKENS
    transcript_tokens = episode_title_tokens(text)
    matched = title_tokens & transcript_tokens
    # Two independent episode-title tokens plus a bounded positive overage is
    # sufficient here: ASR commonly phoneticizes a brand or surname (for
    # example, Spanx/Fluenz) even when the guest and remaining title tokens
    # identify the correct recording.
    content_match = len(title_tokens) >= 2 and len(matched) >= 2
    return 0 < overage <= MAX_CONTENT_VERIFIED_DURATION_OVERAGE_SECONDS and content_match


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _multipart(fields: Sequence[tuple[str, str]]) -> tuple[bytes, str]:
    boundary = f"pif-{hashlib.sha256(str(fields).encode()).hexdigest()[:24]}"
    chunks: list[bytes] = []
    for name, value in fields:
        chunks.extend(
            [
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode(),
                value.encode(),
                b"\r\n",
            ]
        )
    chunks.append(f"--{boundary}--\r\n".encode())
    return b"".join(chunks), boundary


def select_asr_plan(conn: sqlite3.Connection, source_ids: Sequence[str]) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    shows: dict[str, Any] = {}
    for source_id in source_ids:
        if source_id not in ALLOWED_SOURCE_IDS:
            raise XaiAsrError(f"ASR source is outside the bounded rebuild lane: {source_id}")
        rows = conn.execute(
            """SELECT id, title, published_at, duration_seconds, audio_url
                 FROM episodes
                WHERE source_id=? AND published_at IS NOT NULL
                  AND audio_url IS NOT NULL AND duration_seconds >= ?
                ORDER BY published_at, id""",
            (source_id, MIN_DURATION_SECONDS),
        ).fetchall()
        chosen = select_period_spread(rows, count=EPISODES_PER_SHOW)
        shows[source_id] = [
            {
                "episode_id": str(row["id"]),
                "title": str(row["title"]),
                "published_at": str(row["published_at"]),
                "duration_seconds": int(row["duration_seconds"]),
                "audio_url": unwrap_tracking_url(str(row["audio_url"])),
            }
            for row in chosen
        ]
    return {
        "schema_version": "pif_signal_desk_xai_asr_plan_v1",
        "provider": "xai",
        "endpoint": XAI_STT_ENDPOINT,
        "source_ids": list(source_ids),
        "episodes_per_show": EPISODES_PER_SHOW,
        "full_catalog_asr_allowed": False,
        "estimated_audio_hours": round(
            sum(row["duration_seconds"] for rows in shows.values() for row in rows) / 3600,
            4,
        ),
        "shows": shows,
    }


def _transcribe(row: Mapping[str, Any], *, api_key: str, attempts: int = 4) -> dict[str, Any]:
    body, boundary = _multipart(
        [
            ("format", "true"),
            ("language", "en"),
            ("diarize", "true"),
            ("url", str(row["audio_url"])),
        ]
    )
    request = Request(
        XAI_STT_ENDPOINT,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "User-Agent": "pif-signal-desk-rebuild/1",
        },
    )
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=900) as response:  # noqa: S310 - fixed xAI endpoint
                payload = json.loads(response.read().decode("utf-8"))
            text = str(payload.get("text") or "").strip()
            duration = float(payload.get("duration") or 0)
            if not text or duration <= 0:
                raise XaiAsrError("xAI STT response omitted text or duration")
            expected_duration = int(row["duration_seconds"])
            validation_error = None
            if abs(duration - expected_duration) > max(180, expected_duration * 0.20):
                if not _content_verified_duration_overage(
                    row, text=text, observed_duration=duration
                ):
                    validation_error = "duration_mismatch"
            else:
                try:
                    validate_transcript_ingest(
                        text=text,
                        duration_seconds=expected_duration,
                        episode_id=str(row["episode_id"]),
                    )
                except Exception:  # preserve paid output privately for adjudication
                    validation_error = "plausibility_guard"
            return {
                "text": text,
                "response": payload,
                "duration": duration,
                "validation_error": validation_error,
            }
        except HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 500, 502, 503, 504} or attempt + 1 >= attempts:
                raise XaiAsrError(f"xAI STT HTTP failure: {exc.code}") from exc
        except (URLError, TimeoutError) as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                raise XaiAsrError("xAI STT network failure") from exc
        time.sleep(2 ** attempt)
    raise XaiAsrError("xAI STT exhausted retries") from last_error


def _load_cached_result(row: Mapping[str, Any], output_root: Path) -> dict[str, Any] | None:
    source_root = output_root / str(row["source_id"])
    text_path = source_root / f"{row['episode_id']}.txt"
    raw_path = source_root / f"{row['episode_id']}.xai-stt.json"
    meta_path = source_root / f"{row['episode_id']}.meta.json"
    if not (text_path.is_file() and raw_path.is_file() and meta_path.is_file()):
        return None
    text_value = text_path.read_text(encoding="utf-8")
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if (
        metadata.get("episode_id") != row["episode_id"]
        or metadata.get("source_id") != row["source_id"]
        or metadata.get("audio_url_sha256") != _sha(str(row["audio_url"]))
        or metadata.get("text_sha256") != _sha(text_value)
    ):
        return None
    validation_error = metadata.get("validation_error")
    observed_duration = float(metadata["observed_duration_seconds"])
    if validation_error == "duration_mismatch" and _content_verified_duration_overage(
        row, text=text_value, observed_duration=observed_duration
    ):
        validation_error = None
    if validation_error is not None:
        return None
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    return {
        "text": text_value.rstrip("\n"),
        "response": payload,
        "duration": observed_duration,
        "validation_error": None,
        "cache_hit": True,
    }


def run_asr(
    *,
    conn: sqlite3.Connection,
    source_ids: Sequence[str],
    output_root: Path,
    api_key: str,
    concurrency: int = MAX_CONCURRENCY,
) -> dict[str, Any]:
    if not api_key:
        raise XaiAsrError("XAI_API_KEY is required")
    if not 1 <= concurrency <= MAX_CONCURRENCY:
        raise XaiAsrError(f"concurrency must be between 1 and {MAX_CONCURRENCY}")
    plan = select_asr_plan(conn, source_ids)
    output_root.mkdir(parents=True, exist_ok=True)
    all_rows = [
        {**row, "source_id": source_id}
        for source_id, rows in plan["shows"].items()
        for row in rows
    ]
    results: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []

    def persist(row: Mapping[str, Any], result: Mapping[str, Any]) -> None:
        source_root = output_root / str(row["source_id"])
        text_path = source_root / f"{row['episode_id']}.txt"
        raw_path = source_root / f"{row['episode_id']}.xai-stt.json"
        meta_path = source_root / f"{row['episode_id']}.meta.json"
        text_value = str(result["text"]) + "\n"
        write_text_atomic(text_path, text_value)
        write_text_atomic(raw_path, json.dumps(result["response"], sort_keys=True) + "\n")
        metadata = {
            "schema_version": "pif_signal_desk_xai_asr_episode_v1",
            "episode_id": row["episode_id"],
            "source_id": row["source_id"],
            "catalog_duration_seconds": row["duration_seconds"],
            "observed_duration_seconds": round(float(result["duration"]), 2),
            "audio_url_sha256": _sha(str(row["audio_url"])),
            "text_sha256": _sha(text_value),
            "validation_error": result.get("validation_error"),
        }
        write_text_atomic(meta_path, json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        if result.get("validation_error"):
            failures.append(metadata)
            return
        results[str(row["episode_id"])] = {
            **row,
            "path": str(text_path.resolve()),
            "response_path": str(raw_path.resolve()),
            "meta_path": str(meta_path.resolve()),
            "sha256": metadata["text_sha256"],
            "observed_duration_seconds": metadata["observed_duration_seconds"],
            "audio_url_sha256": metadata["audio_url_sha256"],
        }

    cached = []
    pending = []
    for row in all_rows:
        cached_result = _load_cached_result(row, output_root)
        if cached_result is None:
            pending.append(row)
        else:
            cached.append((row, cached_result))
    for row, result in cached:
        persist(row, result)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_transcribe, row, api_key=api_key): row for row in pending}
        for future in as_completed(futures):
            row = futures[future]
            try:
                persist(row, future.result())
            except Exception as exc:  # finish the bounded batch and retain every success
                failures.append(
                    {
                        "episode_id": row["episode_id"],
                        "source_id": row["source_id"],
                        "validation_error": type(exc).__name__,
                    }
                )

    if failures:
        failure_receipt = {
            "schema_version": "pif_signal_desk_xai_asr_failures_v1",
            "created_at": now_iso(),
            "failed_count": len(failures),
            "failures": failures,
            "successful_count": len(results),
            "canonical_database_mutated": False,
        }
        write_text_atomic(
            output_root / "failures.json",
            json.dumps(failure_receipt, indent=2, sort_keys=True) + "\n",
        )
        raise XaiAsrError(
            f"xAI STT batch retained {len(results)} valid outputs and quarantined "
            f"{len(failures)} failures"
        )

    receipts: dict[str, Any] = {}
    for source_id in source_ids:
        selected = []
        for row in plan["shows"][source_id]:
            result = results[str(row["episode_id"])]
            selected.append(
                {
                    "id": result["episode_id"],
                    "title": result["title"],
                    "published_at": result["published_at"],
                    "duration": result["duration_seconds"],
                    "observed_duration_seconds": result["observed_duration_seconds"],
                    "path": result["path"],
                    "response_path": result["response_path"],
                    "meta_path": result["meta_path"],
                    "sha256": result["sha256"],
                    "audio_url_sha256": result["audio_url_sha256"],
                    "source_kind": "xai_stt_rest_diarized_flattened_text",
                }
            )
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "created_at": now_iso(),
            "source_id": source_id,
            "provider": "xai",
            "endpoint": XAI_STT_ENDPOINT,
            "ready_for_benchmark_overlay": True,
            "selected": selected,
            "estimated_cost_usd": round(
                sum(row["observed_duration_seconds"] for row in selected)
                / 3600
                * XAI_STT_RATE_USD_PER_HOUR,
                4,
            ),
            "canonical_database_mutated": False,
            "privacy": "private_benchmark_transcript_and_word_timestamps_no_public_aggregation",
        }
        receipt["receipt_sha256"] = _sha(
            json.dumps(receipt, sort_keys=True, separators=(",", ":"))
        )
        path = output_root / source_id / "receipt.json"
        write_text_atomic(path, json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        receipts[source_id] = {"path": str(path.resolve()), **receipt}
    return {
        "schema_version": "pif_signal_desk_xai_asr_run_v1",
        "plan": {key: value for key, value in plan.items() if key != "shows"},
        "receipts": receipts,
        "total_estimated_cost_usd": round(
            sum(row["estimated_cost_usd"] for row in receipts.values()), 4
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/factory.sqlite"))
    parser.add_argument("--source", action="append", required=True, choices=sorted(ALLOWED_SOURCE_IDS))
    parser.add_argument("--output-root", type=Path, default=Path("work/signal-desk-rebuild/acquisition/xai-asr"))
    parser.add_argument("--concurrency", type=int, default=MAX_CONCURRENCY)
    args = parser.parse_args(argv)
    api_key = os.environ.get("XAI_API_KEY") or getpass.getpass("XAI_API_KEY: ")
    conn = sqlite3.connect(args.database.resolve())
    try:
        result = run_asr(
            conn=conn,
            source_ids=tuple(args.source),
            output_root=args.output_root.resolve(),
            api_key=api_key,
            concurrency=args.concurrency,
        )
    finally:
        conn.close()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
