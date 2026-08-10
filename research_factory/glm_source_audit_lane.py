"""Development-only source-aware audit and density diagnosis for frozen GLM candidates."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .glm_candidate_lane import (
    DEVELOPMENT_SEGMENTS,
    SEALED_EPISODE_ID,
    _candidate_events,
    _canonical_json,
    _numeric_distribution,
    _read_object,
    _replace_owned_json,
    _sha256_file,
    _sha256_text,
    _span_overlap_metrics,
    _write_new_json,
    fair_match_events,
)
from .glm_workhorse import (
    EXTRACTION_AGENT_PROMPT,
    _codex_usage_from_jsonl,
    _timeout_output,
    build_private_job,
    score_source_audit,
    source_audit_schema,
)
from .labels import _validate_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AUDIT_MODEL = "gpt-5.5"
AUDIT_REASONING = "low"
AUDIT_CALL_CEILING = 45
AUDIT_INSTRUCTIONS = (
    "Audit every candidate directly against its evidence-eligible transcript. "
    "support=supported only when the claim is fully entailed, partial when directionally "
    "grounded but overstated or missing a qualification, unsupported otherwise. "
    "distinctness=duplicate for semantic restatements of another candidate in the same case "
    "and fragment when it is not independently useful. signal=useful only when the proposition "
    "has concrete downstream research or graph value rather than being setup, banter, a bare "
    "mention, or an ordinary term. type_fit=neighbor when a defensible adjacent ontology family "
    "fits but the chosen type is not best. verdict=keep for usable as-is, revise for a grounded "
    "candidate needing claim/type cleanup, and drop for unsupported, duplicate, fragmentary, or "
    "low-signal candidates. Judge the transcript, not the frozen reference set."
)
THROTTLE_MARKERS = ("429", "rate limit", "rate_limit", "too many requests", "throttl")


class SourceAuditLaneError(RuntimeError):
    """Fail-closed audit-lane error."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_shadow_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    shadow = (PROJECT_ROOT / "shadow" / "scoring").resolve()
    if shadow not in resolved.parents:
        raise SourceAuditLaneError(f"output must be beneath shadow/scoring: {resolved}")
    return resolved


def _audit_cases(manifest: Mapping[str, Any], candidate_root: Path) -> list[dict[str, Any]]:
    entries = list(manifest.get("folds", {}).get("DEVELOPMENT", {}).get("entries") or [])
    if len(entries) != DEVELOPMENT_SEGMENTS:
        raise SourceAuditLaneError("development fold no longer contains exactly 60 segments")
    cases = []
    for entry in entries:
        if str(entry.get("episode_id")) == SEALED_EPISODE_ID:
            raise SourceAuditLaneError("sealed episode appeared in development fold")
        text_path = Path(str(entry["segment_text_path"])).expanduser().resolve()
        text = text_path.read_text(encoding="utf-8")
        if _sha256_text(text) != str(entry["segment_text_sha256"]):
            raise SourceAuditLaneError(f"segment hash changed: {entry['segment_id']}")
        candidates = _candidate_events(candidate_root, str(entry["segment_id"]))
        cases.append(
            {
                "case_id": str(entry["segment_id"]),
                "episode_id": str(entry["episode_id"]),
                "source_id": str(entry["source_id"]),
                "evidence_eligible_extract_text": text,
                "candidates": [
                    {
                        "candidate_id": index,
                        "event_type": event.get("event_type"),
                        "claim_text": event.get("claim_text"),
                        "evidence": event.get("evidence"),
                    }
                    for index, event in enumerate(candidates)
                ],
            }
        )
    return cases


def _balanced_batches(
    cases: Sequence[Mapping[str, Any]], *, max_cases: int = 4, max_candidates: int = 42
) -> list[list[dict[str, Any]]]:
    nonempty = [dict(case) for case in cases if case.get("candidates")]
    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_candidates = 0
    for case in nonempty:
        count = len(case["candidates"])
        if current and (len(current) >= max_cases or current_candidates + count > max_candidates):
            batches.append(current)
            current = []
            current_candidates = 0
        current.append(case)
        current_candidates += count
    if current:
        batches.append(current)
    return batches


def initialize_source_audit(
    *, manifest_path: Path, candidate_root: Path, output_root: Path
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    candidates = candidate_root.expanduser().resolve()
    root = _require_shadow_path(output_root)
    if root.exists():
        raise FileExistsError(root)
    manifest = _read_object(manifest_file)
    cases = _audit_cases(manifest, candidates)
    batches = _balanced_batches(cases)
    if len(batches) > AUDIT_CALL_CEILING:
        raise SourceAuditLaneError("planned batches exceed authorized Codex call ceiling")
    root.mkdir(parents=True)
    os.chmod(root, 0o700)
    for name in ("packets", "schemas", "outputs", "raw-events", "stderr", "attempts", "scratch"):
        (root / name).mkdir()
    batch_plan = []
    for index, batch in enumerate(batches, 1):
        batch_id = f"batch-{index:02d}"
        packet = {
            "instructions": AUDIT_INSTRUCTIONS,
            "cases": [
                {
                    "case_id": case["case_id"],
                    "evidence_eligible_extract_text": case["evidence_eligible_extract_text"],
                    "candidates": case["candidates"],
                }
                for case in batch
            ],
        }
        candidate_count = sum(len(case["candidates"]) for case in batch)
        schema = source_audit_schema([str(case["case_id"]) for case in batch], candidate_count)
        packet_path = root / "packets" / f"{batch_id}.private.json"
        schema_path = root / "schemas" / f"{batch_id}.json"
        _write_new_json(packet_path, packet)
        _write_new_json(schema_path, schema)
        batch_plan.append(
            {
                "batch_id": batch_id,
                "case_ids": [str(case["case_id"]) for case in batch],
                "case_count": len(batch),
                "candidate_count": candidate_count,
                "packet_path": str(packet_path),
                "packet_sha256": _sha256_file(packet_path),
                "schema_path": str(schema_path),
                "schema_sha256": _sha256_file(schema_path),
                "state": "pending",
                "attempts": [],
            }
        )
    ledger = {
        "schema_version": "pif_glm52_source_audit_bound_ledger_v1",
        "created_at": _now(),
        "updated_at": _now(),
        "state": "declared_pre_provider",
        "fold": "DEVELOPMENT",
        "manifest_path": str(manifest_file),
        "manifest_sha256": _sha256_file(manifest_file),
        "candidate_root": str(candidates),
        "candidate_count": sum(len(case["candidates"]) for case in cases),
        "segments": len(cases),
        "segments_with_candidates": sum(bool(case["candidates"]) for case in cases),
        "audit_model": AUDIT_MODEL,
        "audit_reasoning_effort": AUDIT_REASONING,
        "codex_call_ceiling": AUDIT_CALL_CEILING,
        "codex_calls_dispatched": 0,
        "codex_calls_succeeded": 0,
        "codex_calls_failed": 0,
        "tokens_used": 0,
        "subscription_spend_usd": 0.0,
        "glm_calls": 0,
        "holdout_calls": 0,
        "extractor_gate_initialized": False,
        "production_database_writes": 0,
        "audit_instructions_sha256": _sha256_text(AUDIT_INSTRUCTIONS),
        "planned_initial_calls": len(batch_plan),
        "batch_plan": batch_plan,
    }
    ledger_path = root / "bound-ledger.json"
    _write_new_json(ledger_path, ledger)
    return {**ledger, "ledger_path": str(ledger_path), "ledger_sha256": _sha256_file(ledger_path)}


def _run_one_audit_call(
    *, root: Path, batch: Mapping[str, Any], attempt_number: int, codex_binary: str, timeout_seconds: int
) -> dict[str, Any]:
    batch_id = str(batch["batch_id"])
    packet_path = Path(str(batch["packet_path"]))
    schema_path = Path(str(batch["schema_path"]))
    packet = _read_object(packet_path)
    output_path = root / "outputs" / f"{batch_id}-attempt-{attempt_number}.private.json"
    command = [
        codex_binary,
        "exec",
        "-m",
        AUDIT_MODEL,
        "-c",
        f'model_reasoning_effort="{AUDIT_REASONING}"',
        "-C",
        str(root / "scratch"),
        "--skip-git-repo-check",
        "--ignore-rules",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(output_path),
        "--json",
        "-",
    ]
    prompt = (
        "Perform the source-aware private candidate audit in the attached packet. "
        "Evaluate every candidate exactly once and return only schema-valid JSON.\n\n"
        "# Private packet\n" + _canonical_json(packet)
    )
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            input=prompt,
            text=True,
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
        )
        stdout, stderr, exit_code, timed_out = (
            completed.stdout,
            completed.stderr,
            completed.returncode,
            False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = _timeout_output(exc.stdout)
        stderr = _timeout_output(exc.stderr)
        exit_code = None
        timed_out = True
    elapsed = round(time.monotonic() - started, 3)
    raw_path = root / "raw-events" / f"{batch_id}-attempt-{attempt_number}.private.jsonl"
    stderr_path = root / "stderr" / f"{batch_id}-attempt-{attempt_number}.private.txt"
    raw_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")
    usage = _codex_usage_from_jsonl(stdout)
    result: dict[str, Any] = {
        "attempt": attempt_number,
        "elapsed_seconds": elapsed,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "usage": usage,
        "raw_path": str(raw_path),
        "stderr_path": str(stderr_path),
        "output_path": str(output_path),
        "schema_valid": False,
        "throttled": any(marker in stderr.lower() for marker in THROTTLE_MARKERS),
    }
    if not timed_out and exit_code == 0 and output_path.is_file():
        try:
            output = _read_object(output_path)
            schema = _read_object(schema_path)
            _validate_schema(schema, output, path="$")
            score_source_audit(packet, output)
            result["schema_valid"] = True
            result["output_sha256"] = _sha256_file(output_path)
        except Exception as exc:
            result["validation_error"] = f"{type(exc).__name__}: {exc}"[:1000]
    return result


def run_source_audit_batches(
    *, ledger_path: Path, codex_binary: str = "codex", timeout_seconds: int = 600
) -> dict[str, Any]:
    ledger_file = ledger_path.expanduser().resolve()
    root = ledger_file.parent
    ledger = _read_object(ledger_file)
    if ledger.get("state") not in {"declared_pre_provider", "audit_in_progress"}:
        raise SourceAuditLaneError(f"ledger is not runnable: {ledger.get('state')}")
    ledger["state"] = "audit_in_progress"
    ledger["updated_at"] = _now()
    _replace_owned_json(ledger_file, ledger)
    for batch in ledger["batch_plan"]:
        if batch.get("state") == "complete":
            continue
        for _retry in range(2):
            if int(ledger["codex_calls_dispatched"]) >= int(ledger["codex_call_ceiling"]):
                ledger.update(state="stopped_call_ceiling", updated_at=_now())
                _replace_owned_json(ledger_file, ledger)
                raise SourceAuditLaneError("Codex call ceiling reached")
            attempt_number = len(batch["attempts"]) + 1
            ledger["codex_calls_dispatched"] += 1
            ledger["updated_at"] = _now()
            _replace_owned_json(ledger_file, ledger)
            result = _run_one_audit_call(
                root=root,
                batch=batch,
                attempt_number=attempt_number,
                codex_binary=codex_binary,
                timeout_seconds=timeout_seconds,
            )
            batch["attempts"].append(result)
            ledger["tokens_used"] += int((result.get("usage") or {}).get("total_tokens") or 0)
            ledger["updated_at"] = _now()
            if result["throttled"]:
                ledger["codex_calls_failed"] += 1
                ledger["state"] = "stopped_provider_throttling"
                _replace_owned_json(ledger_file, ledger)
                raise SourceAuditLaneError("provider throttling detected")
            if result["schema_valid"]:
                ledger["codex_calls_succeeded"] += 1
                batch["state"] = "complete"
                _replace_owned_json(ledger_file, ledger)
                break
            ledger["codex_calls_failed"] += 1
            batch["state"] = "retry_pending"
            _replace_owned_json(ledger_file, ledger)
        if batch.get("state") != "complete":
            ledger["state"] = "stopped_batch_failure"
            ledger["updated_at"] = _now()
            _replace_owned_json(ledger_file, ledger)
            raise SourceAuditLaneError(f"batch failed twice: {batch['batch_id']}")
    ledger["state"] = "audit_complete_pending_report"
    ledger["updated_at"] = _now()
    _replace_owned_json(ledger_file, ledger)
    return ledger


def finalize_source_audit(*, ledger_path: Path, output_path: Path) -> dict[str, Any]:
    ledger_file = ledger_path.expanduser().resolve()
    ledger = _read_object(ledger_file)
    if ledger.get("state") != "audit_complete_pending_report":
        raise SourceAuditLaneError("audit calls are not complete")
    all_cases: list[dict[str, Any]] = []
    all_evaluations: list[dict[str, Any]] = []
    elapsed = 0.0
    for batch in ledger["batch_plan"]:
        packet = _read_object(Path(str(batch["packet_path"])))
        successful = next(
            attempt for attempt in reversed(batch["attempts"]) if attempt.get("schema_valid")
        )
        output = _read_object(Path(str(successful["output_path"])))
        all_cases.extend(packet["cases"])
        all_evaluations.extend(output["evaluations"])
        elapsed += float(successful["elapsed_seconds"])
    combined_packet = {"instructions": AUDIT_INSTRUCTIONS, "cases": all_cases}
    combined_output = {"evaluations": all_evaluations}
    score = score_source_audit(combined_packet, combined_output)
    if score["candidate_count"] != int(ledger["candidate_count"]):
        raise SourceAuditLaneError("combined audit coverage mismatch")
    gate = {
        "all_frozen_candidates_audited": score["candidate_count"] == int(ledger["candidate_count"]),
        "supported_or_partial_at_least_0.95": score["supported_or_partial_fraction"] >= 0.95,
        "actionable_at_least_0.75": score["actionable_fraction"] >= 0.75,
        "clean_keep_at_least_0.60": score["clean_keep_fraction"] >= 0.60,
    }
    gate["passed"] = all(gate.values())
    report = {
        "schema_version": "pif_glm52_phase3_source_aware_audit_v1",
        "created_at": _now(),
        "fold": "DEVELOPMENT",
        "manifest_path": ledger["manifest_path"],
        "manifest_sha256": ledger["manifest_sha256"],
        "candidate_root": ledger["candidate_root"],
        "judge_model": ledger["audit_model"],
        "reasoning_effort": ledger["audit_reasoning_effort"],
        "reference_set_used_for_candidate_validity": False,
        "candidate_count": ledger["candidate_count"],
        "segments": ledger["segments"],
        "codex_calls": ledger["codex_calls_dispatched"],
        "codex_call_ceiling": ledger["codex_call_ceiling"],
        "tokens_used": ledger["tokens_used"],
        "subscription_spend_usd": 0.0,
        "successful_call_elapsed_seconds": round(elapsed, 3),
        "score": score,
        "gate": gate,
        "canary_reference": {
            "candidate_count": 108,
            "verdict": {"keep": 82, "revise": 21, "drop": 5},
            "support": {"supported": 101, "partial": 7, "unsupported": 0},
            "distinctness": {"distinct": 103, "fragment": 5, "duplicate": 0},
            "signal": {"useful": 102, "low_signal": 6},
            "type_fit": {"correct": 92, "neighbor": 16, "wrong": 0},
        },
        "holdout_opened": False,
        "extractor_gate_initialized": False,
        "production_database_writes": 0,
        "evaluations": all_evaluations,
    }
    output_file = _require_shadow_path(output_path)
    _write_new_json(output_file, report)
    ledger.update(
        state="complete",
        updated_at=_now(),
        report_path=str(output_file),
        report_sha256=_sha256_file(output_file),
    )
    _replace_owned_json(ledger_file, ledger)
    return {**report, "path": str(output_file), "file_sha256": _sha256_file(output_file)}


def _pearson(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right))
    denominator = math.sqrt(
        sum((a - left_mean) ** 2 for a in left) * sum((b - right_mean) ** 2 for b in right)
    )
    return numerator / denominator if denominator else 0.0


def _rank(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: values[index])
    ranks = [0.0] * len(values)
    cursor = 0
    while cursor < len(order):
        end = cursor + 1
        while end < len(order) and values[order[end]] == values[order[cursor]]:
            end += 1
        rank = (cursor + end - 1) / 2 + 1
        for index in order[cursor:end]:
            ranks[index] = rank
        cursor = end
    return ranks


def _correlations(left: Sequence[int], right: Sequence[int]) -> dict[str, float]:
    a, b = list(map(float, left)), list(map(float, right))
    return {
        "pearson": round(_pearson(a, b), 6),
        "spearman": round(_pearson(_rank(a), _rank(b)), 6),
    }


def _position_bin(fraction: float) -> str:
    index = min(4, max(0, int(fraction * 5)))
    return ("0_20", "20_40", "40_60", "60_80", "80_100")[index]


def _quartile_summaries(rows: Sequence[Mapping[str, Any]], key: str) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (float(row[key]), str(row["segment_id"])))
    summaries = []
    for quartile in range(4):
        start = len(ordered) * quartile // 4
        end = len(ordered) * (quartile + 1) // 4
        group = ordered[start:end]
        golden = sum(int(row["golden_events"]) for row in group)
        glm = sum(int(row["glm_events"]) for row in group)
        matches = sum(int(row["fair_matches"]) for row in group)
        summaries.append(
            {
                "quartile": quartile + 1,
                "segments": len(group),
                f"mean_{key}": round(statistics.fmean(float(row[key]) for row in group), 6),
                "golden_events_per_segment": round(golden / len(group), 6),
                "glm_events_per_segment": round(glm / len(group), 6),
                "glm_to_golden_event_ratio": round(glm / golden, 6) if golden else None,
                "fair_match_recall": round(matches / golden, 6) if golden else None,
            }
        )
    return summaries


def diagnose_density_gap(
    *, manifest_path: Path, candidate_root: Path, output_path: Path
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    candidates_root = candidate_root.expanduser().resolve()
    manifest = _read_object(manifest_file)
    entries = list(manifest.get("folds", {}).get("DEVELOPMENT", {}).get("entries") or [])
    if len(entries) != DEVELOPMENT_SEGMENTS:
        raise SourceAuditLaneError("development fold changed")
    type_totals: Counter[str] = Counter()
    type_missed: Counter[str] = Counter()
    char_totals: Counter[str] = Counter()
    char_missed: Counter[str] = Counter()
    ordinal_totals: Counter[str] = Counter()
    ordinal_missed: Counter[str] = Counter()
    candidate_counts: list[int] = []
    golden_counts: list[int] = []
    text_lengths: list[int] = []
    candidate_position_bins: Counter[str] = Counter()
    golden_position_bins: Counter[str] = Counter()
    candidate_position_fractions: list[float] = []
    golden_position_fractions: list[float] = []
    full_input_job_matches = 0
    job_files_present = 0
    per_segment = []
    total_matches = 0
    for entry in entries:
        text = Path(str(entry["segment_text_path"])).read_text(encoding="utf-8")
        golden = [dict(event) for event in entry.get("accepted_events") or []]
        candidate = _candidate_events(candidates_root, str(entry["segment_id"]))
        matches = fair_match_events(golden, candidate)
        matched_golden = {item[0] for item in matches}
        total_matches += len(matches)
        ordered = sorted(
            range(len(golden)),
            key=lambda index: (int(golden[index].get("evidence_start") or 0), index),
        )
        ordinal_by_index = {event_index: rank for rank, event_index in enumerate(ordered)}
        for index, event in enumerate(golden):
            event_type = str(event.get("event_type") or "unknown")
            start = int(event.get("evidence_start") or 0)
            char_fraction = start / max(1, len(text))
            char_bin = _position_bin(char_fraction)
            ordinal_fraction = (ordinal_by_index[index] + 0.5) / max(1, len(golden))
            ordinal_bin = _position_bin(ordinal_fraction)
            type_totals[event_type] += 1
            char_totals[char_bin] += 1
            ordinal_totals[ordinal_bin] += 1
            golden_position_bins[char_bin] += 1
            golden_position_fractions.append(char_fraction)
            if index not in matched_golden:
                type_missed[event_type] += 1
                char_missed[char_bin] += 1
                ordinal_missed[ordinal_bin] += 1
        candidate_starts = []
        candidate_ends = []
        for event in candidate:
            evidence = str(event.get("evidence") or "")
            start = text.find(evidence)
            if start >= 0:
                end = start + len(evidence)
                candidate_starts.append(start)
                candidate_ends.append(end)
                candidate_fraction = start / max(1, len(text))
                candidate_position_bins[_position_bin(candidate_fraction)] += 1
                candidate_position_fractions.append(candidate_fraction)
        job_path = candidates_root / "jobs" / f"{entry['segment_id']}.private.json"
        if job_path.is_file():
            job_files_present += 1
            frozen_job = _read_object(job_path)
            dispatched_text = str(
                frozen_job.get("input", {}).get("evidence_eligible_extract_text") or ""
            )
            full_input_job_matches += int(dispatched_text == text)
        candidate_counts.append(len(candidate))
        golden_counts.append(len(golden))
        text_lengths.append(len(text))
        per_segment.append(
            {
                "segment_id": str(entry["segment_id"]),
                "text_characters": len(text),
                "golden_events": len(golden),
                "glm_events": len(candidate),
                "fair_matches": len(matches),
                "glm_to_golden_ratio": round(len(candidate) / len(golden), 6) if golden else None,
                "last_glm_evidence_end_fraction": round(max(candidate_ends) / len(text), 6)
                if candidate_ends and text
                else None,
            }
        )
    count_histogram = Counter(candidate_counts)
    modes = sorted(key for key, value in count_histogram.items() if value == max(count_histogram.values()))
    span_overlap = []
    for entry in entries:
        text = Path(str(entry["segment_text_path"])).read_text(encoding="utf-8")
        span_overlap.extend(
            _span_overlap_metrics(
                [dict(event) for event in entry.get("accepted_events") or []],
                _candidate_events(candidates_root, str(entry["segment_id"])),
                segment_text=text,
            )
        )
    first_entry = entries[0]
    from .glm_candidate_lane import _case_from_entry

    job = build_private_job(_case_from_entry(first_entry, manifest_path=manifest_file), max_events=50)
    coverage_wording = [
        instruction
        for instruction in job["instructions"]
        if any(marker in instruction for marker in ("Read every word", "First inventory", "Before returning"))
    ]
    report = {
        "schema_version": "pif_glm52_phase3_density_diagnosis_v1",
        "created_at": _now(),
        "fold": "DEVELOPMENT",
        "provider_calls": 0,
        "manifest_path": str(manifest_file),
        "manifest_sha256": _sha256_file(manifest_file),
        "candidate_root": str(candidates_root),
        "counts": {
            "segments": len(entries),
            "golden_events": sum(golden_counts),
            "glm_events": sum(candidate_counts),
            "fair_matches": total_matches,
            "unmatched_golden_events": sum(golden_counts) - total_matches,
            "glm_events_per_segment": round(statistics.fmean(candidate_counts), 6),
            "golden_events_per_segment": round(statistics.fmean(golden_counts), 6),
        },
        "prompt_coverage_contract": {
            "exhaustive_instruction_present": True,
            "extraction_agent_wording": EXTRACTION_AGENT_PROMPT,
            "operative_job_wording": coverage_wording,
            "max_events": 50,
            "observed_max_events": max(candidate_counts),
            "cap_binding": max(candidate_counts) == 50,
        },
        "unmatched_by_event_type": {
            event_type: {
                "golden": type_totals[event_type],
                "missed": type_missed[event_type],
                "matched": type_totals[event_type] - type_missed[event_type],
                "miss_rate": round(type_missed[event_type] / type_totals[event_type], 6),
                "share_of_all_misses": round(type_missed[event_type] / sum(type_missed.values()), 6),
            }
            for event_type in sorted(type_totals)
        },
        "unmatched_by_character_position_quintile": {
            key: {
                "golden": char_totals[key],
                "missed": char_missed[key],
                "miss_rate": round(char_missed[key] / char_totals[key], 6),
            }
            for key in ("0_20", "20_40", "40_60", "60_80", "80_100")
        },
        "unmatched_by_golden_ordinal_quintile": {
            key: {
                "golden": ordinal_totals[key],
                "missed": ordinal_missed[key],
                "miss_rate": round(ordinal_missed[key] / ordinal_totals[key], 6),
            }
            for key in ("0_20", "20_40", "40_60", "60_80", "80_100")
        },
        "self_limiting_checks": {
            "dispatched_input_integrity": {
                "job_files_present": job_files_present,
                "full_text_exact_matches": full_input_job_matches,
                "all_jobs_contained_full_hash_pinned_segment": full_input_job_matches == len(entries),
            },
            "glm_event_count_histogram": {str(key): count_histogram[key] for key in sorted(count_histogram)},
            "glm_event_count_modes": modes,
            "segments_at_round_counts": {
                str(value): sum(count == value for count in candidate_counts)
                for value in (5, 10, 15, 20, 25, 30, 35, 50)
            },
            "segments_at_configured_cap": sum(count == 50 for count in candidate_counts),
            "correlation_glm_count_vs_segment_characters": _correlations(candidate_counts, text_lengths),
            "correlation_glm_count_vs_golden_count": _correlations(candidate_counts, golden_counts),
            "correlation_golden_count_vs_segment_characters": _correlations(golden_counts, text_lengths),
            "candidate_evidence_character_position_quintiles": dict(candidate_position_bins),
            "golden_evidence_character_position_quintiles": dict(golden_position_bins),
            "candidate_evidence_in_first_40_percent_fraction": round(
                sum(value < 0.40 for value in candidate_position_fractions)
                / len(candidate_position_fractions),
                6,
            ),
            "candidate_evidence_in_first_half_fraction": round(
                sum(value < 0.50 for value in candidate_position_fractions)
                / len(candidate_position_fractions),
                6,
            ),
            "golden_evidence_in_first_half_fraction": round(
                sum(value < 0.50 for value in golden_position_fractions)
                / len(golden_position_fractions),
                6,
            ),
            "last_glm_evidence_end_fraction_distribution": _numeric_distribution(
                [
                    float(row["last_glm_evidence_end_fraction"])
                    for row in per_segment
                    if row["last_glm_evidence_end_fraction"] is not None
                ]
            ),
            "by_segment_length_quartile": _quartile_summaries(per_segment, "text_characters"),
            "by_golden_density_quartile": _quartile_summaries(per_segment, "golden_events"),
        },
        "evidence_span_overlap": {
            "substantial_overlap_count": sum(item["overlap_coefficient"] >= 0.50 for item in span_overlap),
            "substantial_overlap_fraction": round(
                sum(item["overlap_coefficient"] >= 0.50 for item in span_overlap) / len(span_overlap), 6
            ),
        },
        "per_segment": per_segment,
        "holdout_opened": False,
        "extractor_gate_initialized": False,
        "production_database_writes": 0,
    }
    output_file = _require_shadow_path(output_path)
    _write_new_json(output_file, report)
    return {**report, "path": str(output_file), "file_sha256": _sha256_file(output_file)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pif-glm-source-audit-lane")
    sub = parser.add_subparsers(dest="action", required=True)
    init = sub.add_parser("init")
    init.add_argument("--manifest", required=True)
    init.add_argument("--candidate-root", required=True)
    init.add_argument("--output-root", required=True)
    run = sub.add_parser("run")
    run.add_argument("--ledger", required=True)
    run.add_argument("--codex-binary", default="codex")
    run.add_argument("--timeout-seconds", type=int, default=600)
    finalize = sub.add_parser("finalize")
    finalize.add_argument("--ledger", required=True)
    finalize.add_argument("--output", required=True)
    diagnose = sub.add_parser("diagnose")
    diagnose.add_argument("--manifest", required=True)
    diagnose.add_argument("--candidate-root", required=True)
    diagnose.add_argument("--output", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.action == "init":
        result = initialize_source_audit(
            manifest_path=Path(args.manifest),
            candidate_root=Path(args.candidate_root),
            output_root=Path(args.output_root),
        )
    elif args.action == "run":
        result = run_source_audit_batches(
            ledger_path=Path(args.ledger),
            codex_binary=args.codex_binary,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.action == "finalize":
        result = finalize_source_audit(
            ledger_path=Path(args.ledger), output_path=Path(args.output)
        )
    else:
        result = diagnose_density_gap(
            manifest_path=Path(args.manifest),
            candidate_root=Path(args.candidate_root),
            output_path=Path(args.output),
        )
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
