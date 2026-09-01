"""Deterministic, transcript-private sanity gate for rebuild ASR fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from .signal_desk_rebuild_xai_asr import asr_contract
from .util import now_iso, write_text_atomic


SCHEMA_VERSION = "pif_signal_desk_rebuild_asr_sanity_v1"
MIN_WORDS_PER_MINUTE = 80.0
MAX_WORDS_PER_MINUTE = 220.0
MAX_TEXT_GAP_SECONDS = 30.0
MAX_IDENTICAL_WORD_RUN = 10

# Only people/organizations explicitly present in episode-title metadata are
# checked. Missing spellings are reported to gold authoring but do not reject
# production-shaped ASR text.
EXPECTED_METADATA_TERMS: Mapping[str, tuple[str, ...]] = {
    "ep_50dc9e86b6d51623bf6d8ee6": ("Sara", "Blakely", "Spanx"),
    "ep_b5ceefe446a979a38ae078c2": ("Sonia", "Gil", "Fluenz"),
    "ep_f1a84a99329b71b67d90d8db": ("Hernan", "Lopez", "Wondery"),
    "ep_b02aba014687b638c0fc3e2c": ("Nicole", "Bernard", "Dawes", "Late July"),
    "ep_3888bcb569f926b8f467717d": ("Nathan", "Benaich"),
    "ep_388c26306754fc6ef3b07c63": ("Laurence", "Liew", "AI Singapore"),
    "ep_59e954ffc06e20f696f92b86": ("Nathan", "Benaich"),
}


class AsrSanityError(RuntimeError):
    pass


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _normalized(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", value.casefold()))


def _max_identical_run(words: Sequence[Mapping[str, Any]]) -> int:
    maximum = current = 0
    previous = None
    for word in words:
        token = _normalized(str(word.get("text") or ""))
        if token and token == previous:
            current += 1
        else:
            current = 1 if token else 0
        previous = token
        maximum = max(maximum, current)
    return maximum


def evaluate_episode(row: Mapping[str, Any]) -> dict[str, Any]:
    episode_id = str(row["id"])
    text_path = Path(str(row["path"])).expanduser().resolve()
    response_path = Path(str(row["response_path"])).expanduser().resolve()
    text_bytes = text_path.read_bytes()
    response = json.loads(response_path.read_text(encoding="utf-8"))
    failures: list[str] = []
    required = set(asr_contract()["response"]["required_top_level_fields"])
    if not required <= response.keys():
        failures.append("response_shape")
    if str(response.get("language") or "").casefold() != "en":
        failures.append("language_mismatch")
    text = text_bytes.decode("utf-8")
    if text != str(response.get("text") or "").strip() + "\n":
        failures.append("post_processing_drift")
    if _sha_bytes(text_bytes) != str(row.get("sha256") or ""):
        failures.append("text_hash")
    duration = float(response.get("duration") or 0)
    words = response.get("words") if isinstance(response.get("words"), list) else []
    if duration <= 0 or not words:
        failures.append("empty_timed_response")
    starts = [float(word.get("start", -1)) for word in words]
    ends = [float(word.get("end", -1)) for word in words]
    if any(start < 0 or end < start for start, end in zip(starts, ends)):
        failures.append("invalid_word_timestamp")
    if any(next_start < end for end, next_start in zip(ends, starts[1:])):
        failures.append("nonmonotonic_word_timestamp")
    if any(not isinstance(word.get("speaker"), int) for word in words):
        failures.append("missing_diarization_label")
    wpm = len(words) / (duration / 60) if duration else 0.0
    if not MIN_WORDS_PER_MINUTE <= wpm <= MAX_WORDS_PER_MINUTE:
        failures.append("words_per_minute")
    gaps: list[tuple[float, float]] = []
    if words and duration:
        gaps = [(0.0, starts[0])]
        gaps.extend((end, start) for end, start in zip(ends, starts[1:]))
        gaps.append((ends[-1], duration))
    long_gaps = [
        {"start": round(start, 2), "end": round(end, 2), "seconds": round(end - start, 2)}
        for start, end in gaps
        if end - start > MAX_TEXT_GAP_SECONDS
    ]
    if long_gaps:
        failures.append("text_gap_over_30_seconds")
    max_run = _max_identical_run(words)
    if max_run > MAX_IDENTICAL_WORD_RUN:
        failures.append("repeated_word_hallucination")
    normalized_text = _normalized(text)
    terms = EXPECTED_METADATA_TERMS.get(episode_id, ())
    padded_text = f" {normalized_text} "
    matched_terms = [
        term for term in terms if f" {_normalized(term)} " in padded_text
    ]
    missing_terms = [term for term in terms if term not in matched_terms]
    return {
        "episode_id": episode_id,
        "source_id": str(row.get("source_id") or ""),
        "passed": not failures,
        "failures": failures,
        "duration_seconds": round(duration, 2),
        "word_count": len(words),
        "words_per_minute": round(wpm, 2),
        "max_text_gap_seconds": round(max((end - start for start, end in gaps), default=0), 2),
        "long_gaps": long_gaps,
        "speaker_label_count": len({word.get("speaker") for word in words}),
        "max_identical_word_run": max_run,
        "metadata_terms_checked": list(terms),
        "metadata_terms_matched": matched_terms,
        "metadata_terms_missing_or_phonetic": missing_terms,
        "text_sha256": _sha_bytes(text_bytes),
    }


def evaluate_receipts(receipt_paths: Sequence[Path], *, output_path: Path) -> dict[str, Any]:
    expected_contract = asr_contract()["contract_sha256"]
    episodes: list[dict[str, Any]] = []
    for path in receipt_paths:
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if receipt.get("asr_contract_sha256") != expected_contract:
            raise AsrSanityError("ASR receipt does not match the frozen production contract")
        source_id = str(receipt.get("source_id") or "")
        for selected in receipt.get("selected") or ():
            if (
                selected.get("transcript_structure") != "asr_diarized"
                or selected.get("source_kind")
                != "xai_stt_rest_asr_diarized_word_timestamps"
            ):
                raise AsrSanityError("ASR episode is missing the pinned structure/source tag")
            episodes.append(evaluate_episode({**selected, "source_id": source_id}))
    episodes.sort(key=lambda row: (row["source_id"], row["episode_id"]))
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "asr_contract_sha256": expected_contract,
        "episode_count": len(episodes),
        "passed_count": sum(bool(row["passed"]) for row in episodes),
        "failed_count": sum(not bool(row["passed"]) for row in episodes),
        "passed": bool(episodes) and all(bool(row["passed"]) for row in episodes),
        "thresholds": {
            "words_per_minute": [MIN_WORDS_PER_MINUTE, MAX_WORDS_PER_MINUTE],
            "max_text_gap_seconds": MAX_TEXT_GAP_SECONDS,
            "max_identical_word_run": MAX_IDENTICAL_WORD_RUN,
        },
        "episodes": episodes,
        "privacy": "sanitized_metrics_hashes_and_metadata_terms_only_no_transcript_text",
    }
    receipt["receipt_sha256"] = _sha_bytes(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    )
    write_text_atomic(output_path, json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    if not receipt["passed"]:
        raise AsrSanityError(
            f"ASR sanity failed closed: {receipt['failed_count']} of {receipt['episode_count']} episodes"
        )
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    result = evaluate_receipts(
        tuple(path.resolve() for path in args.receipt), output_path=args.output.resolve()
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
