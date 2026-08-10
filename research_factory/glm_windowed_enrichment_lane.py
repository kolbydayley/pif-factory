"""Shadow-only GLM window coverage and Codex enrichment measurement lane."""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import shutil
import statistics
import subprocess
import time
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .glm_candidate_lane import (
    DEVELOPMENT_SEGMENTS,
    LABEL_PACK,
    SEALED_EPISODE_ID,
    _candidate_events,
    _canonical_json,
    _numeric_distribution,
    _read_object,
    _replace_owned_json,
    _sha256_file,
    _sha256_text,
    _write_new_json,
    fair_match_events,
)
from .glm_workhorse import (
    DEFAULT_MODEL,
    CanaryCase,
    _codex_usage_from_jsonl,
    _run_case_opencode,
    _timeout_output,
)
from .labels import (
    _validate_schema,
    label_pack_provenance,
    load_label_pack,
    repair_label_output_for_submission,
    render_prompt,
    validate_label_output,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WINDOW_CHARACTERS = 1846
WINDOW_OVERLAP_CHARACTERS = 185
WINDOW_STRIDE_CHARACTERS = WINDOW_CHARACTERS - WINDOW_OVERLAP_CHARACTERS
EXPECTED_WINDOW_CALLS = 163
GLM_CALL_CEILING = 180
GLM_CONCURRENCY = 3
MAX_EVENTS_PER_WINDOW = 50
CODEX_MODEL = "gpt-5.5"
CODEX_REASONING = "high"
CODEX_CALL_CEILING = 25
THROTTLE_MARKERS = (
    "429",
    "rate limit",
    "rate_limit",
    "too many requests",
    "throttl",
    "usage limit",
    "purchase more credits",
)


class WindowedLaneError(RuntimeError):
    """Fail-closed window/enrichment error."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_shadow(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    shadow = (PROJECT_ROOT / "shadow" / "scoring").resolve()
    if shadow not in resolved.parents:
        raise WindowedLaneError(f"output must be beneath shadow/scoring: {resolved}")
    return resolved


def plan_segment_windows(text_length: int) -> list[tuple[int, int]]:
    if text_length < 0:
        raise ValueError("negative text length")
    if text_length == 0:
        return [(0, 0)]
    windows = []
    start = 0
    while True:
        end = min(text_length, start + WINDOW_CHARACTERS)
        windows.append((start, end))
        if end == text_length:
            break
        start += WINDOW_STRIDE_CHARACTERS
    return windows


def _manifest_entries(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    entries = [dict(row) for row in manifest.get("folds", {}).get("DEVELOPMENT", {}).get("entries") or []]
    if len(entries) != DEVELOPMENT_SEGMENTS:
        raise WindowedLaneError("development fold changed")
    if any(str(row.get("episode_id")) == SEALED_EPISODE_ID for row in entries):
        raise WindowedLaneError("sealed episode appeared in development fold")
    return entries


def initialize_window_ledger(
    *, manifest_path: Path, output_root: Path
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    root = _require_shadow(output_root)
    if root.exists():
        raise FileExistsError(root)
    manifest = _read_object(manifest_file)
    entries = _manifest_entries(manifest)
    root.mkdir(parents=True)
    os.chmod(root, 0o700)
    for name in ("jobs", "raw-events", "raw-answers", "stderr", "validated", "scratch", "worker-state", "window-results", "merged", "merged/validated", "enrichment"):
        (root / name).mkdir(parents=True, exist_ok=True)
    windows = []
    coverage_checks = []
    for entry in entries:
        text_path = Path(str(entry["segment_text_path"])).expanduser().resolve()
        text = text_path.read_text(encoding="utf-8")
        if _sha256_text(text) != str(entry["segment_text_sha256"]):
            raise WindowedLaneError(f"segment hash changed: {entry['segment_id']}")
        planned = plan_segment_windows(len(text))
        if planned[0][0] != 0 or planned[-1][1] != len(text):
            raise WindowedLaneError("window endpoints do not cover segment")
        if any(right[0] > left[1] for left, right in zip(planned, planned[1:])):
            raise WindowedLaneError("window plan contains a gap")
        coverage_checks.append(
            {
                "segment_id": str(entry["segment_id"]),
                "text_characters": len(text),
                "window_count": len(planned),
                "first_start": planned[0][0],
                "last_end": planned[-1][1],
                "minimum_overlap": min((left[1] - right[0] for left, right in zip(planned, planned[1:])), default=0),
                "complete_union": True,
            }
        )
        for index, (start, end) in enumerate(planned):
            window_id = f"{entry['segment_id']}__w{index:02d}"
            window_text = text[start:end]
            windows.append(
                {
                    "window_id": window_id,
                    "segment_id": str(entry["segment_id"]),
                    "episode_id": str(entry["episode_id"]),
                    "source_name": str(entry["source_name"]),
                    "segment_index": int(entry["segment_index"]),
                    "window_index": index,
                    "window_start": start,
                    "window_end": end,
                    "window_characters": len(window_text),
                    "window_text_sha256": _sha256_text(window_text),
                    "segment_text_path": str(text_path),
                    "segment_text_sha256": str(entry["segment_text_sha256"]),
                    "state": "pending",
                    "attempts": [],
                }
            )
    if len(windows) != EXPECTED_WINDOW_CALLS:
        raise WindowedLaneError(f"expected {EXPECTED_WINDOW_CALLS} windows, found {len(windows)}")
    plan = {
        "schema_version": "pif_glm52_window_plan_v1",
        "created_at": _now(),
        "manifest_path": str(manifest_file),
        "manifest_sha256": _sha256_file(manifest_file),
        "window_characters": WINDOW_CHARACTERS,
        "overlap_characters": WINDOW_OVERLAP_CHARACTERS,
        "stride_characters": WINDOW_STRIDE_CHARACTERS,
        "window_count": len(windows),
        "coverage_checks": coverage_checks,
        "windows": windows,
        "holdout_opened": False,
    }
    plan_path = root / "window-plan.json"
    _write_new_json(plan_path, plan)
    ledger = {
        "schema_version": "pif_glm52_windowed_bound_ledger_v1",
        "created_at": _now(),
        "updated_at": _now(),
        "state": "declared_pre_provider",
        "manifest_path": str(manifest_file),
        "manifest_sha256": _sha256_file(manifest_file),
        "window_plan_path": str(plan_path),
        "window_plan_sha256": _sha256_file(plan_path),
        "model": DEFAULT_MODEL,
        "transport": "opencode",
        "billing_lane": "z_ai_coding_plan_subscription_only",
        "planned_initial_calls": len(windows),
        "glm_call_ceiling": GLM_CALL_CEILING,
        "glm_calls_dispatched": 0,
        "glm_calls_succeeded": 0,
        "glm_calls_failed": 0,
        "calibration_calls": 3,
        "calibration_total_tokens": [],
        "measured_token_ceiling": None,
        "tokens_used": 0,
        "subscription_spend_usd": 0.0,
        "codex_calls": 0,
        "holdout_calls": 0,
        "extractor_gate_initialized": False,
        "production_database_writes": 0,
    }
    ledger_path = root / "glm-bound-ledger.json"
    _write_new_json(ledger_path, ledger)
    return {**ledger, "ledger_path": str(ledger_path), "ledger_sha256": _sha256_file(ledger_path)}


def _window_case(window: Mapping[str, Any], *, retry_number: int = 0) -> CanaryCase:
    text = Path(str(window["segment_text_path"])).read_text(encoding="utf-8")
    if _sha256_text(text) != str(window["segment_text_sha256"]):
        raise WindowedLaneError(f"segment changed: {window['segment_id']}")
    window_text = text[int(window["window_start"]):int(window["window_end"])]
    if _sha256_text(window_text) != str(window["window_text_sha256"]):
        raise WindowedLaneError(f"window changed: {window['window_id']}")
    call_id = str(window["window_id"]) + (f"__r{retry_number}" if retry_number else "")
    return CanaryCase(
        segment_id=call_id,
        source_name=str(window["source_name"]),
        chunk_index=int(window["window_index"]),
        extract_text=window_text,
        left_context="",
        right_context="",
        episode_context={
            "episode_id": str(window["episode_id"]),
            "original_segment_id": str(window["segment_id"]),
            "original_window_start": int(window["window_start"]),
            "original_window_end": int(window["window_end"]),
        },
        adjacent_segments=(),
        baseline_events=(),
        prompt_path=Path(str(window["segment_text_path"])),
        baseline_path=Path(str(window["segment_text_path"])),
    )


def _usage_tokens(result: Mapping[str, Any]) -> int:
    return sum(int((attempt.get("usage") or {}).get("total_tokens") or 0) for attempt in result.get("attempts") or [])


def _result_throttled(result: Mapping[str, Any], root: Path) -> bool:
    for attempt in result.get("attempts") or []:
        p = root / "stderr" / f"{result['segment_id']}-attempt-{attempt['attempt']}.private.txt"
        text = p.read_text(encoding="utf-8", errors="replace").lower() if p.is_file() else ""
        if any(marker in text for marker in THROTTLE_MARKERS):
            return True
    return False


def _dispatch_window(window: Mapping[str, Any], root: Path, retry_number: int) -> dict[str, Any]:
    return _run_case_opencode(
        _window_case(window, retry_number=retry_number),
        output_root=root,
        model=DEFAULT_MODEL,
        timeout_seconds=240,
        retry_count=0,
        max_events=MAX_EVENTS_PER_WINDOW,
        opencode_binary="/opt/homebrew/bin/opencode",
    )


def run_windowed_glm(*, ledger_path: Path) -> dict[str, Any]:
    ledger_file = ledger_path.expanduser().resolve()
    root = ledger_file.parent
    ledger = _read_object(ledger_file)
    if ledger.get("state") != "declared_pre_provider":
        raise WindowedLaneError("GLM ledger is not pre-provider")
    plan_path = Path(str(ledger["window_plan_path"]))
    plan = _read_object(plan_path)
    windows = plan["windows"]
    ledger.update(state="calibrating", updated_at=_now())
    _replace_owned_json(ledger_file, ledger)

    def record(window: dict[str, Any], result: dict[str, Any], retry_number: int) -> None:
        tokens = _usage_tokens(result)
        attempt_record = {
            "retry_number": retry_number,
            "call_id": str(result["segment_id"]),
            "usable": bool(result.get("usable")),
            "event_count": int(result.get("event_count") or 0),
            "tokens": tokens,
            "elapsed_seconds": sum(float(a.get("elapsed_seconds") or 0) for a in result.get("attempts") or []),
            "result_path": str(root / "window-results" / f"{result['segment_id']}.json"),
        }
        _write_new_json(Path(attempt_record["result_path"]), result)
        window["attempts"].append(attempt_record)
        ledger["tokens_used"] += tokens
        if result.get("usable"):
            window["state"] = "complete"
            ledger["glm_calls_succeeded"] += 1
        else:
            window["state"] = "retry_pending"
            ledger["glm_calls_failed"] += 1
        if _result_throttled(result, root):
            ledger["state"] = "stopped_provider_throttling"
        ledger["updated_at"] = _now()
        _replace_owned_json(plan_path, plan)
        ledger["window_plan_sha256"] = _sha256_file(plan_path)
        _replace_owned_json(ledger_file, ledger)

    for window in windows[:3]:
        ledger["glm_calls_dispatched"] += 1
        ledger["updated_at"] = _now()
        _replace_owned_json(ledger_file, ledger)
        result = _dispatch_window(window, root, 0)
        record(window, result, 0)
        if ledger["state"] == "stopped_provider_throttling":
            raise WindowedLaneError("provider throttling during calibration")
        if not result.get("usable") or _usage_tokens(result) <= 0:
            ledger.update(state="stopped_calibration_failure", updated_at=_now())
            _replace_owned_json(ledger_file, ledger)
            raise WindowedLaneError("calibration call was not usable with complete usage")
        ledger["calibration_total_tokens"].append(_usage_tokens(result))
    ledger["measured_token_ceiling"] = math.ceil(max(ledger["calibration_total_tokens"]) * GLM_CALL_CEILING * 1.15)
    ledger["state"] = "running"
    ledger["updated_at"] = _now()
    _replace_owned_json(ledger_file, ledger)

    pending = windows[3:]
    with ThreadPoolExecutor(max_workers=GLM_CONCURRENCY) as executor:
        active: dict[Any, dict[str, Any]] = {}
        cursor = 0
        while cursor < len(pending) or active:
            while cursor < len(pending) and len(active) < GLM_CONCURRENCY:
                if ledger["glm_calls_dispatched"] >= GLM_CALL_CEILING:
                    raise WindowedLaneError("GLM call ceiling reached")
                window = pending[cursor]
                cursor += 1
                ledger["glm_calls_dispatched"] += 1
                ledger["updated_at"] = _now()
                _replace_owned_json(ledger_file, ledger)
                active[executor.submit(_dispatch_window, window, root, 0)] = window
            completed, _ = wait(active, return_when=FIRST_COMPLETED)
            for future in completed:
                window = active.pop(future)
                result = future.result()
                record(window, result, 0)
                if ledger["state"] == "stopped_provider_throttling":
                    raise WindowedLaneError("provider throttling")
                if ledger["tokens_used"] > ledger["measured_token_ceiling"]:
                    ledger.update(state="stopped_token_ceiling", updated_at=_now())
                    _replace_owned_json(ledger_file, ledger)
                    raise WindowedLaneError("measured token ceiling exceeded")
    failures = [window for window in windows if window["state"] != "complete"]
    for window in failures:
        if ledger["glm_calls_dispatched"] >= GLM_CALL_CEILING:
            ledger.update(state="stopped_call_ceiling", updated_at=_now())
            _replace_owned_json(ledger_file, ledger)
            raise WindowedLaneError("retry headroom exhausted")
        ledger["glm_calls_dispatched"] += 1
        ledger["updated_at"] = _now()
        _replace_owned_json(ledger_file, ledger)
        result = _dispatch_window(window, root, 1)
        record(window, result, 1)
        if not result.get("usable"):
            ledger.update(state="stopped_window_failure", updated_at=_now())
            _replace_owned_json(ledger_file, ledger)
            raise WindowedLaneError(f"window failed twice: {window['window_id']}")
    ledger.update(state="glm_complete_pending_merge", updated_at=_now())
    _replace_owned_json(ledger_file, ledger)
    return ledger


def _candidate_occurrences(text: str, evidence: str, start: int, end: int) -> list[int]:
    positions = []
    cursor = text.find(evidence, start, end)
    while cursor >= 0 and cursor + len(evidence) <= end:
        positions.append(cursor)
        cursor = text.find(evidence, cursor + 1, end)
    return positions


def _overlap_coefficient(a: Mapping[str, Any], b: Mapping[str, Any]) -> float:
    start = max(int(a["evidence_start"]), int(b["evidence_start"]))
    end = min(int(a["evidence_end"]), int(b["evidence_end"]))
    intersection = max(0, end - start)
    denominator = min(int(a["evidence_end"]) - int(a["evidence_start"]), int(b["evidence_end"]) - int(b["evidence_start"]))
    return intersection / denominator if denominator else 0.0


def _is_overlap_duplicate(candidate: Mapping[str, Any], kept: Mapping[str, Any]) -> bool:
    if candidate["window_id"] == kept["window_id"]:
        return False
    exact = (
        candidate["event_type"] == kept["event_type"]
        and candidate["evidence_start"] == kept["evidence_start"]
        and candidate["evidence_end"] == kept["evidence_end"]
        and candidate["claim_text"] == kept["claim_text"]
    )
    semantic_overlap = (
        candidate["event_type"] == kept["event_type"]
        and _overlap_coefficient(candidate, kept) >= 0.80
    )
    return exact or semantic_overlap


def _position_bin(fraction: float) -> str:
    return ("0_20", "20_40", "40_60", "60_80", "80_100")[min(4, int(max(0.0, fraction) * 5))]


def merge_and_score_windows(
    *, manifest_path: Path, ledger_path: Path, output_path: Path
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    ledger_file = ledger_path.expanduser().resolve()
    root = ledger_file.parent
    ledger = _read_object(ledger_file)
    if ledger.get("state") != "glm_complete_pending_merge":
        raise WindowedLaneError("GLM windows are not complete")
    manifest = _read_object(manifest_file)
    entries = _manifest_entries(manifest)
    plan = _read_object(Path(str(ledger["window_plan_path"])))
    by_segment: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for window in plan["windows"]:
        by_segment[str(window["segment_id"])].append(window)
    raw_total = 0
    invalid_offsets = 0
    ambiguous_offsets = 0
    duplicates = 0
    dedup_total = 0
    all_position_totals: Counter[str] = Counter()
    all_position_missed: Counter[str] = Counter()
    segment_rows = []
    per_window_elapsed = []
    per_window_tokens = []
    for entry in entries:
        segment_id = str(entry["segment_id"])
        text = Path(str(entry["segment_text_path"])).read_text(encoding="utf-8")
        mapped = []
        segment_elapsed = 0.0
        segment_tokens = 0
        for window in by_segment[segment_id]:
            successful = next(attempt for attempt in reversed(window["attempts"]) if attempt["usable"])
            call_id = str(successful["call_id"])
            payload = _read_object(root / "validated" / f"{call_id}.private.json")
            segment_elapsed += float(successful["elapsed_seconds"])
            segment_tokens += int(successful["tokens"])
            per_window_elapsed.append(float(successful["elapsed_seconds"])); per_window_tokens.append(int(successful["tokens"]))
            for event in payload.get("events") or []:
                raw_total += 1
                evidence = str(event.get("evidence") or "")
                occurrences = _candidate_occurrences(text, evidence, int(window["window_start"]), int(window["window_end"]))
                if len(occurrences) != 1:
                    ambiguous_offsets += int(len(occurrences) > 1)
                    invalid_offsets += int(len(occurrences) == 0)
                    continue
                start = occurrences[0]; end = start + len(evidence)
                if text[start:end] != evidence:
                    invalid_offsets += 1
                    continue
                mapped.append({**event, "evidence_start": start, "evidence_end": end, "window_id": str(window["window_id"]), "window_start": int(window["window_start"]), "window_end": int(window["window_end"])})
        mapped.sort(key=lambda event: (int(event["evidence_start"]), int(event["evidence_end"]), str(event["event_type"]), str(event["claim_text"])))
        kept = []
        for event in mapped:
            if any(_is_overlap_duplicate(event, prior) for prior in kept):
                duplicates += 1
                continue
            kept.append(event)
        dedup_total += len(kept)
        merged_payload = {"segment_id": segment_id, "episode_id": str(entry["episode_id"]), "events": kept}
        _write_new_json(root / "merged" / "validated" / f"{segment_id}.private.json", merged_payload)
        golden = [dict(event) for event in entry.get("accepted_events") or []]
        matches = fair_match_events(golden, kept)
        matched_golden = {match[0] for match in matches}
        for index, event in enumerate(golden):
            fraction = int(event.get("evidence_start") or 0) / max(1, len(text))
            bucket = _position_bin(fraction)
            all_position_totals[bucket] += 1
            if index not in matched_golden:
                all_position_missed[bucket] += 1
        segment_rows.append({
            "segment_id": segment_id,
            "window_count": len(by_segment[segment_id]),
            "raw_events": len(mapped),
            "deduplicated_events": len(kept),
            "golden_events": len(golden),
            "fair_matches": len(matches),
            "elapsed_seconds": round(segment_elapsed, 3),
            "tokens": segment_tokens,
            "last_evidence_end_fraction": round(max((int(e["evidence_end"]) for e in kept), default=0) / max(1, len(text)), 6),
        })
    if invalid_offsets or ambiguous_offsets:
        offset_status = "valid_after_fail_closed_drops"
    else:
        offset_status = "all_raw_candidates_mapped_uniquely"
    position = {
        key: {
            "golden": all_position_totals[key],
            "missed": all_position_missed[key],
            "miss_rate": round(all_position_missed[key] / all_position_totals[key], 6),
        }
        for key in ("0_20", "20_40", "40_60", "60_80", "80_100")
    }
    report = {
        "schema_version": "pif_glm52_windowed_development_report_v1",
        "created_at": _now(),
        "fold": "DEVELOPMENT",
        "window_configuration": {"characters": WINDOW_CHARACTERS, "overlap_characters": WINDOW_OVERLAP_CHARACTERS, "stride_characters": WINDOW_STRIDE_CHARACTERS, "planned_windows": EXPECTED_WINDOW_CALLS},
        "usage": {
            "glm_calls": ledger["glm_calls_dispatched"],
            "glm_call_ceiling": ledger["glm_call_ceiling"],
            "tokens": ledger["tokens_used"],
            "subscription_spend_usd": 0.0,
            "wall_time_per_window": _numeric_distribution(per_window_elapsed),
            "tokens_per_window": _numeric_distribution(per_window_tokens),
            "wall_time_per_segment": _numeric_distribution([row["elapsed_seconds"] for row in segment_rows]),
            "tokens_per_segment": _numeric_distribution([row["tokens"] for row in segment_rows]),
        },
        "events": {
            "raw_window_events": raw_total,
            "deduplicated_events": dedup_total,
            "events_per_segment": round(dedup_total / len(entries), 6),
            "codex_golden_events": sum(int(entry["event_count"]) for entry in entries),
            "codex_events_per_segment": round(sum(int(entry["event_count"]) for entry in entries) / len(entries), 6),
            "overlap_duplicates_removed": duplicates,
            "duplicate_rate_of_mapped_raw": round(duplicates / max(1, raw_total - invalid_offsets - ambiguous_offsets), 6),
        },
        "offset_validation": {"status": offset_status, "raw_candidates": raw_total, "unique_valid_original_offsets": raw_total - invalid_offsets - ambiguous_offsets, "not_found_dropped": invalid_offsets, "ambiguous_dropped": ambiguous_offsets, "valid_fraction": round((raw_total - invalid_offsets - ambiguous_offsets) / max(1, raw_total), 6)},
        "positional_miss_rate": position,
        "segments": segment_rows,
        "merged_candidate_root": str(root / "merged"),
        "holdout_opened": False,
        "extractor_gate_initialized": False,
        "production_database_writes": 0,
    }
    output_file = _require_shadow(output_path)
    _write_new_json(output_file, report)
    ledger.update(state="glm_windowed_complete", updated_at=_now(), report_path=str(output_file), report_sha256=_sha256_file(output_file))
    _replace_owned_json(ledger_file, ledger)
    return {**report, "path": str(output_file), "file_sha256": _sha256_file(output_file)}


def _enrichment_prompt(entry: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]]) -> str:
    text = Path(str(entry["segment_text_path"])).read_text(encoding="utf-8")
    context = {
        "mode": "enrichment_only",
        "segment_id": str(entry["segment_id"]),
        "episode_id": str(entry["episode_id"]),
        "source_name": str(entry["source_name"]),
        "candidate_contract": (
            "The supplied candidates are the fixed discovery inventory. Enrich each candidate into one full discourse_event. "
            "Do not perform a new discovery pass and do not add unrelated propositions. Preserve candidate evidence exactly, "
            "preserve its original evidence_start/evidence_end, and populate actor, speaker, reported actor, target, entities, "
            "claim_type, metrics, attribution, confidence, quality flags, and all remaining schema fields. Reject only a candidate "
            "that cannot form a valid grounded v3.1 event, and explain it in rejected_candidates."
        ),
        "glm_candidates": [
            {key: candidate.get(key) for key in ("event_type", "evidence", "evidence_start", "evidence_end", "claim_text")}
            for candidate in candidates
        ],
    }
    return render_prompt(LABEL_PACK, {"text": text}, context)


def initialize_enrichment_ledger(
    *, manifest_path: Path, window_report_path: Path, window_root: Path, output_root: Path, sample_size: int = 12
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve(); report_file = window_report_path.expanduser().resolve(); merged_root = window_root.expanduser().resolve(); root = _require_shadow(output_root)
    if root.exists(): raise FileExistsError(root)
    manifest = _read_object(manifest_file); report = _read_object(report_file); entries = _manifest_entries(manifest)
    by_id = {str(entry["segment_id"]): entry for entry in entries}
    eligible = [row for row in report["segments"] if row["last_evidence_end_fraction"] >= 0.80 and row["fair_matches"] >= 1 and row["deduplicated_events"] >= 1]
    eligible.sort(key=lambda row: (-float(row["last_evidence_end_fraction"]), -int(row["fair_matches"]), str(row["segment_id"])))
    selected = eligible[:sample_size]
    if len(selected) < min(8, sample_size): raise WindowedLaneError("too few restored-coverage segments for enrichment sample")
    root.mkdir(parents=True); os.chmod(root,0o700)
    for name in ("prompts","schemas","outputs","raw-events","stderr","scratch","validated"): (root/name).mkdir()
    pack = load_label_pack(LABEL_PACK)
    cases=[]
    for row in selected:
        segment_id=str(row["segment_id"]); entry=by_id[segment_id]
        candidate_payload=_read_object(merged_root/"merged"/"validated"/f"{segment_id}.private.json")
        prompt=_enrichment_prompt(entry,candidate_payload["events"])
        prompt_path=root/"prompts"/f"{segment_id}.private.md"; prompt_path.write_text(prompt,encoding="utf-8"); os.chmod(prompt_path,0o600)
        schema_path=root/"schemas"/f"{segment_id}.json"; _write_new_json(schema_path,pack.schema)
        cases.append({"segment_id":segment_id,"episode_id":str(entry["episode_id"]),"segment_text_path":str(entry["segment_text_path"]),"segment_text_sha256":str(entry["segment_text_sha256"]),"candidate_count":len(candidate_payload["events"]),"prompt_path":str(prompt_path),"prompt_sha256":_sha256_file(prompt_path),"schema_path":str(schema_path),"state":"pending","attempts":[]})
    ledger={"schema_version":"pif_glm52_codex_enrichment_bound_ledger_v1","created_at":_now(),"updated_at":_now(),"state":"declared_pre_provider","model":CODEX_MODEL,"reasoning_effort":CODEX_REASONING,"codex_call_ceiling":CODEX_CALL_CEILING,"codex_calls_dispatched":0,"codex_calls_succeeded":0,"codex_calls_failed":0,"tokens_used":0,"subscription_spend_usd":0.0,"sample_size":len(cases),"selection_rule":"restored coverage: last GLM evidence end >=0.80, at least one fair match, nonempty candidates; rank by coverage then matches","label_pack_provenance":label_pack_provenance(pack),"manifest_path":str(manifest_file),"manifest_sha256":_sha256_file(manifest_file),"window_report_path":str(report_file),"window_report_sha256":_sha256_file(report_file),"cases":cases,"glm_calls":0,"holdout_calls":0,"extractor_gate_initialized":False,"production_database_writes":0}
    ledger_path=root/"codex-bound-ledger.json"; _write_new_json(ledger_path,ledger)
    return {**ledger,"ledger_path":str(ledger_path),"ledger_sha256":_sha256_file(ledger_path)}


def _run_enrichment_case(case: Mapping[str, Any], root: Path, attempt: int) -> dict[str, Any]:
    output_path=root/"outputs"/f"{case['segment_id']}-attempt-{attempt}.private.json"
    command=["codex","exec","-m",CODEX_MODEL,"-c",f'model_reasoning_effort="{CODEX_REASONING}"',"-C",str(root/"scratch"),"--skip-git-repo-check","--ignore-rules","--ephemeral","--sandbox","read-only","--output-schema",str(case["schema_path"]),"--output-last-message",str(output_path),"--json","-"]
    prompt=Path(str(case["prompt_path"])).read_text(encoding="utf-8")
    started=time.monotonic()
    try:
        done=subprocess.run(command,cwd=str(PROJECT_ROOT),input=prompt,text=True,errors="replace",stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=600,check=False)
        stdout,stderr,exit_code,timed_out=done.stdout,done.stderr,done.returncode,False
    except subprocess.TimeoutExpired as exc:
        stdout,stderr,exit_code,timed_out=_timeout_output(exc.stdout),_timeout_output(exc.stderr),None,True
    elapsed=round(time.monotonic()-started,3)
    (root/"raw-events"/f"{case['segment_id']}-attempt-{attempt}.private.jsonl").write_text(stdout,encoding="utf-8")
    (root/"stderr"/f"{case['segment_id']}-attempt-{attempt}.private.txt").write_text(stderr,encoding="utf-8")
    provider_text = (stderr + "\n" + stdout).lower()
    result={"attempt":attempt,"elapsed_seconds":elapsed,"exit_code":exit_code,"timed_out":timed_out,"usage":_codex_usage_from_jsonl(stdout),"output_path":str(output_path),"schema_valid":False,"raw_contract_valid":False,"usable_after_repair":False,"throttled":any(marker in provider_text for marker in THROTTLE_MARKERS)}
    if not timed_out and exit_code==0 and output_path.is_file():
        try:
            payload=_read_object(output_path); text=Path(str(case["segment_text_path"])).read_text(encoding="utf-8")
            _validate_schema(load_label_pack(LABEL_PACK).schema,payload,path="$"); result["schema_valid"]=True
            try: validate_label_output(LABEL_PACK,payload,segment_text=text); result["raw_contract_valid"]=True
            except Exception as exc: result["raw_validation_error"]=f"{type(exc).__name__}: {exc}"[:1000]
            repaired=copy.deepcopy(payload); repairs=repair_label_output_for_submission(LABEL_PACK,repaired,segment_text=text); validate_label_output(LABEL_PACK,repaired,segment_text=text)
            result.update(usable_after_repair=True,repair_count=repairs,event_count=len(repaired.get("discourse_events") or []),output_sha256=_sha256_file(output_path))
            _write_new_json(root/"validated"/f"{case['segment_id']}.private.json",repaired)
        except Exception as exc: result["validation_error"]=f"{type(exc).__name__}: {exc}"[:1000]
    return result


def run_enrichment(*, ledger_path: Path) -> dict[str, Any]:
    ledger_file=ledger_path.expanduser().resolve(); root=ledger_file.parent; ledger=_read_object(ledger_file)
    if ledger.get("state")!="declared_pre_provider": raise WindowedLaneError("enrichment ledger not pre-provider")
    ledger.update(state="running",updated_at=_now()); _replace_owned_json(ledger_file,ledger)
    for case in ledger["cases"]:
        for attempt in (1,2):
            if ledger["codex_calls_dispatched"]>=ledger["codex_call_ceiling"]: raise WindowedLaneError("Codex call ceiling reached")
            ledger["codex_calls_dispatched"]+=1; ledger["updated_at"]=_now(); _replace_owned_json(ledger_file,ledger)
            result=_run_enrichment_case(case,root,attempt); case["attempts"].append(result); ledger["tokens_used"]+=int((result.get("usage") or {}).get("total_tokens") or 0)
            if result["throttled"]:
                ledger.update(state="stopped_provider_throttling",updated_at=_now()); _replace_owned_json(ledger_file,ledger); raise WindowedLaneError("Codex throttling")
            if result["usable_after_repair"]:
                case["state"]="complete"; ledger["codex_calls_succeeded"]+=1; break
            ledger["codex_calls_failed"]+=1; case["state"]="retry_pending"
            ledger["updated_at"]=_now(); _replace_owned_json(ledger_file,ledger)
        if case["state"]!="complete":
            ledger.update(state="stopped_enrichment_failure",updated_at=_now()); _replace_owned_json(ledger_file,ledger); raise WindowedLaneError(f"enrichment failed twice: {case['segment_id']}")
        ledger["updated_at"]=_now(); _replace_owned_json(ledger_file,ledger)
    ledger.update(state="enrichment_complete_pending_report",updated_at=_now()); _replace_owned_json(ledger_file,ledger); return ledger


def finalize_enrichment(*, ledger_path: Path, output_path: Path) -> dict[str, Any]:
    ledger_file=ledger_path.expanduser().resolve(); root=ledger_file.parent; ledger=_read_object(ledger_file)
    if ledger.get("state")!="enrichment_complete_pending_report": raise WindowedLaneError("enrichment incomplete")
    successful=[next(a for a in reversed(case["attempts"]) if a["usable_after_repair"]) for case in ledger["cases"]]
    elapsed=[float(a["elapsed_seconds"]) for a in successful]; tokens=[int((a.get("usage") or {}).get("total_tokens") or 0) for a in successful]
    event_ratios=[]
    for case in ledger["cases"]:
        payload=_read_object(root/"validated"/f"{case['segment_id']}.private.json"); event_ratios.append(len(payload.get("discourse_events") or [])/max(1,int(case["candidate_count"])))
    mean_elapsed=statistics.fmean(elapsed); mean_tokens=statistics.fmean(tokens)
    report={"schema_version":"pif_glm52_codex_enrichment_report_v1","created_at":_now(),"fold":"DEVELOPMENT","sample_size":len(successful),"usage":{"codex_calls":ledger["codex_calls_dispatched"],"codex_call_ceiling":ledger["codex_call_ceiling"],"tokens":ledger["tokens_used"],"subscription_spend_usd":0.0,"wall_time_seconds":_numeric_distribution(elapsed),"billed_tokens":_numeric_distribution(tokens)},"quality":{"schema_valid_calls":sum(a["schema_valid"] for a in successful),"raw_contract_valid_calls":sum(a["raw_contract_valid"] for a in successful),"usable_after_repair_calls":sum(a["usable_after_repair"] for a in successful),"total_repairs":sum(int(a.get("repair_count") or 0) for a in successful),"output_to_candidate_event_ratio":_numeric_distribution(event_ratios)},"baseline":{"full_extraction_wall_seconds_per_segment":260.0,"full_extraction_billed_tokens_per_segment":180000,"measured_completions_per_hour":104.43},"comparison":{"wall_time_ratio":round(mean_elapsed/260.0,6),"wall_time_reduction_fraction":round(1-mean_elapsed/260.0,6),"token_ratio":round(mean_tokens/180000,6),"token_reduction_fraction":round(1-mean_tokens/180000,6),"serial_enrichment_completions_per_hour":round(3600/mean_elapsed,6),"throughput_multiplier_vs_measured_codex_only":round((3600/mean_elapsed)/104.43,6)},"cases":[{"segment_id":case["segment_id"],"candidate_count":case["candidate_count"],"result":next(a for a in reversed(case["attempts"]) if a["usable_after_repair"])} for case in ledger["cases"]],"holdout_opened":False,"extractor_gate_initialized":False,"production_database_writes":0}
    output_file=_require_shadow(output_path); _write_new_json(output_file,report); ledger.update(state="complete",updated_at=_now(),report_path=str(output_file),report_sha256=_sha256_file(output_file)); _replace_owned_json(ledger_file,ledger); return {**report,"path":str(output_file),"file_sha256":_sha256_file(output_file)}


def main(argv: Sequence[str] | None = None) -> int:
    parser=argparse.ArgumentParser(); sub=parser.add_subparsers(dest="action",required=True)
    p=sub.add_parser("init-windows"); p.add_argument("--manifest",required=True); p.add_argument("--output-root",required=True)
    p=sub.add_parser("run-windows"); p.add_argument("--ledger",required=True)
    p=sub.add_parser("merge-windows"); p.add_argument("--manifest",required=True); p.add_argument("--ledger",required=True); p.add_argument("--output",required=True)
    p=sub.add_parser("init-enrichment"); p.add_argument("--manifest",required=True); p.add_argument("--window-report",required=True); p.add_argument("--window-root",required=True); p.add_argument("--output-root",required=True); p.add_argument("--sample-size",type=int,default=12)
    p=sub.add_parser("run-enrichment"); p.add_argument("--ledger",required=True)
    p=sub.add_parser("finalize-enrichment"); p.add_argument("--ledger",required=True); p.add_argument("--output",required=True)
    args=parser.parse_args(list(argv) if argv is not None else None)
    if args.action=="init-windows": result=initialize_window_ledger(manifest_path=Path(args.manifest),output_root=Path(args.output_root))
    elif args.action=="run-windows": result=run_windowed_glm(ledger_path=Path(args.ledger))
    elif args.action=="merge-windows": result=merge_and_score_windows(manifest_path=Path(args.manifest),ledger_path=Path(args.ledger),output_path=Path(args.output))
    elif args.action=="init-enrichment": result=initialize_enrichment_ledger(manifest_path=Path(args.manifest),window_report_path=Path(args.window_report),window_root=Path(args.window_root),output_root=Path(args.output_root),sample_size=args.sample_size)
    elif args.action=="run-enrichment": result=run_enrichment(ledger_path=Path(args.ledger))
    else: result=finalize_enrichment(ledger_path=Path(args.ledger),output_path=Path(args.output))
    print(json.dumps(result,ensure_ascii=True,indent=2,sort_keys=True)); return 0


if __name__=="__main__": raise SystemExit(main())
