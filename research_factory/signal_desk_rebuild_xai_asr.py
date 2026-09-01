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
XAI_STT_PROVIDER_MODEL = "xai_speech_to_text_endpoint_unversioned"
XAI_STT_LANGUAGE = "en"
XAI_STT_DIARIZE = True
XAI_STT_FORMAT = True
XAI_STT_FILLER_WORDS = False
XAI_STT_VAD_THRESHOLD = 0.5
XAI_STT_TIMESTAMP_GRANULARITY = "word"
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


def asr_contract() -> dict[str, Any]:
    """Exact provider-exposed contract shared by benchmark and future rump ASR."""
    payload = {
        "schema_version": "pif_signal_desk_xai_asr_contract_v1",
        "provider": "xai",
        "provider_model": XAI_STT_PROVIDER_MODEL,
        "model_selection_surface": "none_exposed_by_provider",
        "endpoint": XAI_STT_ENDPOINT,
        "request": {
            "format": XAI_STT_FORMAT,
            "language": XAI_STT_LANGUAGE,
            "diarize": XAI_STT_DIARIZE,
            "filler_words": XAI_STT_FILLER_WORDS,
            "vad_threshold": XAI_STT_VAD_THRESHOLD,
            "keyterms": [],
            "input": "url",
        },
        "response": {
            "timestamp_granularity": XAI_STT_TIMESTAMP_GRANULARITY,
            "speaker_label_location": "words[].speaker",
            "required_top_level_fields": ["duration", "language", "text", "words"],
        },
        "post_processing": [
            "read_response_text_field",
            "strip_leading_and_trailing_whitespace",
            "append_single_newline",
            "no_case_punctuation_or_name_normalization",
        ],
    }
    payload["contract_sha256"] = _sha(
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
    )
    return payload


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
        "asr_contract": asr_contract(),
        "source_ids": list(source_ids),
        "episodes_per_show": EPISODES_PER_SHOW,
        "full_catalog_asr_allowed": False,
        "estimated_audio_hours": round(
            sum(row["duration_seconds"] for rows in shows.values() for row in rows) / 3600,
            4,
        ),
        "shows": shows,
    }


def select_exact_asr_plan(
    conn: sqlite3.Connection, episode_ids: Sequence[str]
) -> dict[str, Any]:
    """Select at most six explicit replacement candidates per allowed show."""
    if not episode_ids or len(set(episode_ids)) != len(episode_ids):
        raise XaiAsrError("exact ASR episode IDs must be nonempty and unique")
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in episode_ids)
    rows = conn.execute(
        f"""SELECT id, source_id, title, published_at, duration_seconds, audio_url
              FROM episodes
             WHERE id IN ({placeholders})
             ORDER BY source_id, published_at, id""",
        tuple(episode_ids),
    ).fetchall()
    if len(rows) != len(episode_ids):
        raise XaiAsrError("one or more exact ASR episode IDs are absent from the catalog")
    shows: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        source_id = str(row["source_id"])
        if source_id not in ALLOWED_SOURCE_IDS:
            raise XaiAsrError(f"ASR source is outside the bounded rebuild lane: {source_id}")
        if not row["audio_url"] or int(row["duration_seconds"] or 0) < MIN_DURATION_SECONDS:
            raise XaiAsrError("exact ASR episode lacks duration-qualified audio")
        shows.setdefault(source_id, []).append(
            {
                "episode_id": str(row["id"]),
                "title": str(row["title"]),
                "published_at": str(row["published_at"]),
                "duration_seconds": int(row["duration_seconds"]),
                "audio_url": unwrap_tracking_url(str(row["audio_url"])),
            }
        )
    if any(len(rows) > 6 for rows in shows.values()):
        raise XaiAsrError("exact replacement acquisition is capped at six episodes per show")
    return {
        "schema_version": "pif_signal_desk_xai_asr_plan_v1",
        "provider": "xai",
        "endpoint": XAI_STT_ENDPOINT,
        "asr_contract": asr_contract(),
        "source_ids": sorted(shows),
        "episodes_per_show": None,
        "full_catalog_asr_allowed": False,
        "replacement_acquisition": True,
        "estimated_audio_hours": round(
            sum(row["duration_seconds"] for rows in shows.values() for row in rows) / 3600,
            4,
        ),
        "shows": shows,
    }


def _transcribe(row: Mapping[str, Any], *, api_key: str, attempts: int = 4) -> dict[str, Any]:
    body, boundary = _multipart(
        [
            ("format", str(XAI_STT_FORMAT).lower()),
            ("language", XAI_STT_LANGUAGE),
            ("diarize", str(XAI_STT_DIARIZE).lower()),
            ("filler_words", str(XAI_STT_FILLER_WORDS).lower()),
            ("vad_threshold", str(XAI_STT_VAD_THRESHOLD)),
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
    exact_episode_ids: Sequence[str] = (),
) -> dict[str, Any]:
    if not api_key:
        raise XaiAsrError("XAI_API_KEY is required")
    if not 1 <= concurrency <= MAX_CONCURRENCY:
        raise XaiAsrError(f"concurrency must be between 1 and {MAX_CONCURRENCY}")
    plan = (
        select_exact_asr_plan(conn, exact_episode_ids)
        if exact_episode_ids
        else select_asr_plan(conn, source_ids)
    )
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
    for source_id in plan["source_ids"]:
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
                    "source_kind": "xai_stt_rest_asr_diarized_word_timestamps",
                    "transcript_structure": "asr_diarized",
                }
            )
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "created_at": now_iso(),
            "source_id": source_id,
            "provider": "xai",
            "provider_model": XAI_STT_PROVIDER_MODEL,
            "endpoint": XAI_STT_ENDPOINT,
            "asr_contract_sha256": asr_contract()["contract_sha256"],
            "ready_for_benchmark_overlay": len(selected) == EPISODES_PER_SHOW,
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


def pin_existing_receipt(path: Path) -> dict[str, Any]:
    """Bind already-paid benchmark output to the now-frozen production contract."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("provider") != "xai" or payload.get("endpoint") != XAI_STT_ENDPOINT:
        raise XaiAsrError("receipt is not an xAI /v1/stt benchmark receipt")
    selected = list(payload.get("selected") or ())
    if len(selected) != EPISODES_PER_SHOW:
        raise XaiAsrError("pinned ASR receipt must contain exactly four episodes")
    for row in selected:
        raw = json.loads(Path(str(row["response_path"])).read_text(encoding="utf-8"))
        required = set(asr_contract()["response"]["required_top_level_fields"])
        if not required <= raw.keys() or any(
            not isinstance(word.get("speaker"), int) for word in raw.get("words") or ()
        ):
            raise XaiAsrError("existing response does not satisfy the diarized word contract")
        row["source_kind"] = "xai_stt_rest_asr_diarized_word_timestamps"
        row["transcript_structure"] = "asr_diarized"
    payload["selected"] = selected
    payload["provider_model"] = XAI_STT_PROVIDER_MODEL
    payload["asr_contract_sha256"] = asr_contract()["contract_sha256"]
    payload.pop("receipt_sha256", None)
    payload["receipt_sha256"] = _sha(
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
    )
    write_text_atomic(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def curate_receipt(
    *, input_paths: Sequence[Path], source_id: str, episode_ids: Sequence[str], output_path: Path
) -> dict[str, Any]:
    """Create one four-episode final receipt from base and replacement receipts."""
    if len(episode_ids) != EPISODES_PER_SHOW or len(set(episode_ids)) != EPISODES_PER_SHOW:
        raise XaiAsrError("curated receipt requires exactly four unique episode IDs")
    rows: dict[str, dict[str, Any]] = {}
    for path in input_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("source_id") != source_id or payload.get("provider") != "xai":
            raise XaiAsrError("curation input source/provider mismatch")
        for row in payload.get("selected") or ():
            rows[str(row["id"])] = dict(row)
    missing = [episode_id for episode_id in episode_ids if episode_id not in rows]
    if missing:
        raise XaiAsrError("curated episode is absent from input receipts")
    selected = [rows[episode_id] for episode_id in episode_ids]
    for row in selected:
        row["source_kind"] = "xai_stt_rest_asr_diarized_word_timestamps"
        row["transcript_structure"] = "asr_diarized"
    payload = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "created_at": now_iso(),
        "source_id": source_id,
        "provider": "xai",
        "provider_model": XAI_STT_PROVIDER_MODEL,
        "endpoint": XAI_STT_ENDPOINT,
        "asr_contract_sha256": asr_contract()["contract_sha256"],
        "ready_for_benchmark_overlay": True,
        "selected": selected,
        "estimated_cost_usd": round(
            sum(float(row["observed_duration_seconds"]) for row in selected)
            / 3600
            * XAI_STT_RATE_USD_PER_HOUR,
            4,
        ),
        "canonical_database_mutated": False,
        "privacy": "private_benchmark_transcript_and_word_timestamps_no_public_aggregation",
    }
    payload["receipt_sha256"] = _sha(
        json.dumps(payload, sort_keys=True, separators=(",", ":"))
    )
    write_text_atomic(output_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/factory.sqlite"))
    parser.add_argument("--source", action="append", choices=sorted(ALLOWED_SOURCE_IDS))
    parser.add_argument("--exact-episode-id", action="append", default=[])
    parser.add_argument("--pin-existing-receipt", action="append", type=Path, default=[])
    parser.add_argument("--curate-input-receipt", action="append", type=Path, default=[])
    parser.add_argument("--curate-output-receipt", type=Path)
    parser.add_argument("--curate-source")
    parser.add_argument("--curate-episode-id", action="append", default=[])
    parser.add_argument("--output-root", type=Path, default=Path("work/signal-desk-rebuild/acquisition/xai-asr"))
    parser.add_argument("--concurrency", type=int, default=MAX_CONCURRENCY)
    args = parser.parse_args(argv)
    if args.pin_existing_receipt:
        pinned = [pin_existing_receipt(path.resolve()) for path in args.pin_existing_receipt]
        print(json.dumps({"pinned_receipts": pinned}, indent=2, sort_keys=True))
        return 0
    if args.curate_output_receipt:
        if not args.curate_source or not args.curate_input_receipt:
            parser.error("curation requires --curate-source and --curate-input-receipt")
        result = curate_receipt(
            input_paths=tuple(path.resolve() for path in args.curate_input_receipt),
            source_id=args.curate_source,
            episode_ids=tuple(args.curate_episode_id),
            output_path=args.curate_output_receipt.resolve(),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if not args.source and not args.exact_episode_id:
        parser.error("--source or --exact-episode-id is required")
    api_key = os.environ.get("XAI_API_KEY") or getpass.getpass("XAI_API_KEY: ")
    conn = sqlite3.connect(args.database.resolve())
    try:
        result = run_asr(
            conn=conn,
            source_ids=tuple(args.source or ()),
            output_root=args.output_root.resolve(),
            api_key=api_key,
            concurrency=args.concurrency,
            exact_episode_ids=tuple(args.exact_episode_id),
        )
    finally:
        conn.close()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
