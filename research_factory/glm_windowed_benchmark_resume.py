"""Resume the bounded Phase 3 windowed enrichment and audit a random candidate sample."""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .glm_candidate_lane import (
    _candidate_events,
    _numeric_distribution,
    _read_object,
    _replace_owned_json,
    _sha256_file,
    _sha256_text,
    _write_new_json,
)
from .glm_source_audit_lane import (
    AUDIT_INSTRUCTIONS,
    _audit_cases,
    _run_one_audit_call,
)
from .glm_windowed_enrichment_lane import _run_enrichment_case
from .glm_workhorse import score_source_audit, source_audit_schema
from .labels import validate_label_output


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AUDIT_MODEL = "gpt-5.5"
AUDIT_REASONING = "low"
AUDIT_CALL_CEILING = 15
AUDIT_SAMPLE_SEGMENTS = 24
AUDIT_BATCH_SEGMENTS = 2
AUDIT_SEED = "glm52-windowed-source-audit-20260801-v1"
BASELINE_SECONDS = 260.0
BASELINE_TOKENS = 180_000
BASELINE_COMPLETIONS_PER_HOUR = 104.43


class ResumeBenchmarkError(RuntimeError):
    """Fail-closed error for the bounded resume benchmark."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_shadow(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    shadow = (PROJECT_ROOT / "shadow" / "scoring").resolve()
    if shadow not in resolved.parents:
        raise ResumeBenchmarkError(f"path must be beneath shadow/scoring: {resolved}")
    return resolved


def _actual_provider_throttle(case: Mapping[str, Any], result: Mapping[str, Any], root: Path) -> bool:
    """Ignore bare numeric offsets such as evidence_end=4291 in successful JSONL."""
    stderr_path = root / "stderr" / f"{case['segment_id']}-attempt-{result['attempt']}.private.txt"
    stderr = stderr_path.read_text(encoding="utf-8", errors="replace").lower() if stderr_path.is_file() else ""
    strong = ("usage limit", "rate limit", "rate_limit", "too many requests", "throttl", "purchase more credits", "http 429", "status 429")
    if any(marker in stderr for marker in strong):
        return True
    raw_path = root / "raw-events" / f"{case['segment_id']}-attempt-{result['attempt']}.private.jsonl"
    if not raw_path.is_file():
        return False
    for line in raw_path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") not in {"error", "turn.failed"}:
            continue
        rendered = json.dumps(event, ensure_ascii=True).lower()
        if any(marker in rendered for marker in strong):
            return True
    return False


def resume_enrichment(*, ledger_path: Path) -> dict[str, Any]:
    """Resume the existing cumulative 25-call ledger after a documented quota reset."""
    ledger_file = _require_shadow(ledger_path)
    root = ledger_file.parent
    ledger = _read_object(ledger_file)
    if ledger.get("state") == "stopped_provider_throttling":
        # Reconcile the known false-positive where bare "429" matched evidence_end=4291.
        false_positive = next((c for c in ledger["cases"] if c.get("attempts") and c["attempts"][-1].get("throttled") and not _actual_provider_throttle(c, c["attempts"][-1], root)), None)
        if false_positive is None or not false_positive["attempts"][-1].get("usable_after_repair"):
            raise ResumeBenchmarkError("ledger contains a genuine provider-throttle stop")
        false_positive["attempts"][-1]["throttled"] = False
        false_positive["attempts"][-1]["throttle_false_positive"] = "bare numeric 429 occurred inside evidence_end=4291"
        false_positive["state"] = "complete"
        ledger["codex_calls_failed"] -= 1
        ledger["codex_calls_succeeded"] += 1
        ledger["state"] = "stopped_provider_usage_limit"
    if ledger.get("state") != "stopped_provider_usage_limit":
        raise ResumeBenchmarkError(f"ledger is not stopped on usage limit: {ledger.get('state')}")
    if int(ledger.get("codex_call_ceiling") or 0) != 25:
        raise ResumeBenchmarkError("the cumulative enrichment ceiling changed")
    ledger.update(
        state="resumed_after_quota_reset",
        resumed_at=_now(),
        updated_at=_now(),
        resume_note="Provider quota reset reported by review loop; cumulative 25-call ceiling retained.",
    )
    _replace_owned_json(ledger_file, ledger)
    for case in ledger["cases"]:
        if case.get("state") == "complete":
            continue
        fresh_attempts = 0
        while fresh_attempts < 2:
            if int(ledger["codex_calls_dispatched"]) >= int(ledger["codex_call_ceiling"]):
                ledger.update(state="stopped_call_ceiling", updated_at=_now())
                _replace_owned_json(ledger_file, ledger)
                raise ResumeBenchmarkError("cumulative Codex call ceiling reached")
            attempt_number = len(case.get("attempts") or []) + 1
            ledger["codex_calls_dispatched"] += 1
            ledger["updated_at"] = _now()
            _replace_owned_json(ledger_file, ledger)
            result = _run_enrichment_case(case, root, attempt_number)
            result["throttled"] = _actual_provider_throttle(case, result, root)
            case.setdefault("attempts", []).append(result)
            fresh_attempts += 1
            ledger["tokens_used"] += int((result.get("usage") or {}).get("total_tokens") or 0)
            ledger["updated_at"] = _now()
            if result.get("throttled"):
                ledger["codex_calls_failed"] += 1
                ledger["state"] = "stopped_provider_throttling"
                _replace_owned_json(ledger_file, ledger)
                raise ResumeBenchmarkError("provider throttling detected")
            if result.get("usable_after_repair"):
                case["state"] = "complete"
                ledger["codex_calls_succeeded"] += 1
                _replace_owned_json(ledger_file, ledger)
                break
            ledger["codex_calls_failed"] += 1
            case["state"] = "retry_pending"
            _replace_owned_json(ledger_file, ledger)
        if case.get("state") != "complete":
            ledger.update(state="stopped_enrichment_failure", updated_at=_now())
            _replace_owned_json(ledger_file, ledger)
            raise ResumeBenchmarkError(f"enrichment failed twice after reset: {case['segment_id']}")
    ledger.update(state="enrichment_complete_pending_report", updated_at=_now())
    _replace_owned_json(ledger_file, ledger)
    return ledger


def finalize_enrichment_benchmark(*, ledger_path: Path, output_path: Path) -> dict[str, Any]:
    ledger_file = _require_shadow(ledger_path)
    output_file = _require_shadow(output_path)
    root = ledger_file.parent
    ledger = _read_object(ledger_file)
    if ledger.get("state") not in {"enrichment_complete_pending_report", "complete"}:
        raise ResumeBenchmarkError("enrichment is not complete")
    successes = []
    event_ratios = []
    preserved = 0
    candidate_total = 0
    enriched_total = 0
    for case in ledger["cases"]:
        success = next((a for a in reversed(case["attempts"]) if a.get("usable_after_repair")), None)
        if success is None:
            raise ResumeBenchmarkError(f"missing success: {case['segment_id']}")
        successes.append(success)
        enriched = _read_object(root / "validated" / f"{case['segment_id']}.private.json")
        segment_text = Path(str(case["segment_text_path"])).read_text(encoding="utf-8")
        validate_label_output("ai_discourse_v3_1", enriched, segment_text=segment_text)
        events = list(enriched.get("discourse_events") or [])
        candidates = _candidate_events(root.parent / "merged", str(case["segment_id"]))
        candidate_total += len(candidates)
        enriched_total += len(events)
        event_ratios.append(len(events) / max(1, len(candidates)))
        available = [(str(e.get("event_type")), str(e.get("evidence"))) for e in events]
        for candidate in candidates:
            key = (str(candidate.get("event_type")), str(candidate.get("evidence")))
            if key in available:
                preserved += 1
                available.remove(key)
    walls = [float(a["elapsed_seconds"]) for a in successes]
    tokens = [int((a.get("usage") or {}).get("total_tokens") or 0) for a in successes]
    mean_wall = statistics.fmean(walls)
    mean_tokens = statistics.fmean(tokens)
    # Calibrate the simple concurrency-10 estimate to the measured production rate.
    production_efficiency = BASELINE_COMPLETIONS_PER_HOUR / (36_000 / BASELINE_SECONDS)
    implied_rate = (36_000 / mean_wall) * production_efficiency
    report = {
        "schema_version": "pif_glm52_windowed_enrichment_benchmark_v2",
        "created_at": _now(),
        "fold": "DEVELOPMENT",
        "sample_size": len(successes),
        "usage": {
            "codex_calls_cumulative": ledger["codex_calls_dispatched"],
            "codex_call_ceiling": ledger["codex_call_ceiling"],
            "codex_calls_succeeded": ledger["codex_calls_succeeded"],
            "codex_calls_failed": ledger["codex_calls_failed"],
            "tokens_cumulative_including_failed_calls": ledger["tokens_used"],
            "successful_wall_seconds": _numeric_distribution(walls),
            "successful_billed_tokens": _numeric_distribution(tokens),
            "subscription_spend_usd": 0.0,
        },
        "quality": {
            "schema_valid_calls": sum(bool(a.get("schema_valid")) for a in successes),
            "raw_contract_valid_calls": sum(bool(a.get("raw_contract_valid")) for a in successes),
            "usable_after_production_repair_calls": sum(bool(a.get("usable_after_repair")) for a in successes),
            "total_repairs": sum(int(a.get("repair_count") or 0) for a in successes),
            "candidate_events": candidate_total,
            "enriched_events": enriched_total,
            "event_type_and_exact_evidence_preserved": preserved,
            "candidate_preservation_fraction": round(preserved / max(1, candidate_total), 6),
            "output_to_candidate_ratio": _numeric_distribution(event_ratios),
        },
        "baseline": {
            "wall_seconds_per_segment": BASELINE_SECONDS,
            "billed_tokens_per_segment": BASELINE_TOKENS,
            "measured_completions_per_hour_concurrency_10": BASELINE_COMPLETIONS_PER_HOUR,
        },
        "comparison": {
            "mean_wall_ratio": round(mean_wall / BASELINE_SECONDS, 6),
            "mean_wall_reduction_fraction": round(1 - mean_wall / BASELINE_SECONDS, 6),
            "mean_token_ratio": round(mean_tokens / BASELINE_TOKENS, 6),
            "mean_token_reduction_fraction": round(1 - mean_tokens / BASELINE_TOKENS, 6),
            "implied_completions_per_hour_concurrency_10": round(implied_rate, 6),
            "throughput_multiplier": round(implied_rate / BASELINE_COMPLETIONS_PER_HOUR, 6),
        },
        "cases": [{"segment_id": c["segment_id"], "candidate_count": c["candidate_count"], "successful_attempt": next(a for a in reversed(c["attempts"]) if a.get("usable_after_repair"))} for c in ledger["cases"]],
        "holdout_opened": False,
        "extractor_gate_initialized": False,
        "production_database_writes": 0,
    }
    _write_new_json(output_file, report)
    ledger.update(state="complete", updated_at=_now(), benchmark_report_path=str(output_file), benchmark_report_sha256=_sha256_file(output_file))
    _replace_owned_json(ledger_file, ledger)
    return {**report, "path": str(output_file), "file_sha256": _sha256_file(output_file)}


def initialize_windowed_audit(*, manifest_path: Path, candidate_root: Path, output_root: Path) -> dict[str, Any]:
    root = _require_shadow(output_root)
    if root.exists():
        raise FileExistsError(root)
    manifest_file = manifest_path.expanduser().resolve()
    candidate_dir = candidate_root.expanduser().resolve()
    manifest = _read_object(manifest_file)
    all_cases = _audit_cases(manifest, candidate_dir)
    rng = random.Random(AUDIT_SEED)
    selected_ids = set(rng.sample([str(c["case_id"]) for c in all_cases], AUDIT_SAMPLE_SEGMENTS))
    selected = [case for case in all_cases if str(case["case_id"]) in selected_ids]
    selected.sort(key=lambda case: str(case["case_id"]))
    batches = [selected[i:i + AUDIT_BATCH_SEGMENTS] for i in range(0, len(selected), AUDIT_BATCH_SEGMENTS)]
    if len(batches) > AUDIT_CALL_CEILING:
        raise ResumeBenchmarkError("audit plan exceeds call ceiling")
    root.mkdir(parents=True)
    os.chmod(root, 0o700)
    for name in ("packets", "schemas", "outputs", "raw-events", "stderr", "attempts", "scratch"):
        (root / name).mkdir()
    plan = []
    for index, batch in enumerate(batches, 1):
        batch_id = f"batch-{index:02d}"
        packet = {"instructions": AUDIT_INSTRUCTIONS, "cases": [{"case_id": c["case_id"], "evidence_eligible_extract_text": c["evidence_eligible_extract_text"], "candidates": c["candidates"]} for c in batch]}
        count = sum(len(c["candidates"]) for c in batch)
        packet_path = root / "packets" / f"{batch_id}.private.json"
        schema_path = root / "schemas" / f"{batch_id}.json"
        _write_new_json(packet_path, packet)
        _write_new_json(schema_path, source_audit_schema([str(c["case_id"]) for c in batch], count))
        plan.append({"batch_id": batch_id, "case_ids": [str(c["case_id"]) for c in batch], "candidate_count": count, "packet_path": str(packet_path), "packet_sha256": _sha256_file(packet_path), "schema_path": str(schema_path), "schema_sha256": _sha256_file(schema_path), "state": "pending", "attempts": []})
    ledger = {
        "schema_version": "pif_glm52_windowed_random_source_audit_bound_v1",
        "created_at": _now(), "updated_at": _now(), "state": "declared_pre_provider",
        "fold": "DEVELOPMENT", "manifest_path": str(manifest_file), "manifest_sha256": _sha256_file(manifest_file),
        "candidate_root": str(candidate_dir), "sampling_seed": AUDIT_SEED, "sampling_method": "deterministic simple random sample without replacement over 60 development segments",
        "selected_segment_ids": sorted(selected_ids), "segments": len(selected), "candidate_count": sum(len(c["candidates"]) for c in selected),
        "audit_model": AUDIT_MODEL, "audit_reasoning_effort": AUDIT_REASONING, "codex_call_ceiling": AUDIT_CALL_CEILING,
        "planned_initial_calls": len(plan), "codex_calls_dispatched": 0, "codex_calls_succeeded": 0, "codex_calls_failed": 0,
        "tokens_used": 0, "subscription_spend_usd": 0.0, "glm_calls": 0, "holdout_calls": 0, "extractor_gate_initialized": False, "production_database_writes": 0,
        "audit_instructions_sha256": _sha256_text(AUDIT_INSTRUCTIONS), "batch_plan": plan,
    }
    ledger_path = root / "bound-ledger.json"
    _write_new_json(ledger_path, ledger)
    return {**ledger, "ledger_path": str(ledger_path), "ledger_sha256": _sha256_file(ledger_path)}


def run_windowed_audit(*, ledger_path: Path) -> dict[str, Any]:
    ledger_file = _require_shadow(ledger_path)
    root = ledger_file.parent
    ledger = _read_object(ledger_file)
    if ledger.get("state") != "declared_pre_provider":
        raise ResumeBenchmarkError("audit ledger is not pre-provider")
    ledger.update(state="audit_in_progress", updated_at=_now())
    _replace_owned_json(ledger_file, ledger)
    for batch in ledger["batch_plan"]:
        for _ in range(2):
            if ledger["codex_calls_dispatched"] >= ledger["codex_call_ceiling"]:
                raise ResumeBenchmarkError("audit call ceiling reached")
            attempt = len(batch["attempts"]) + 1
            ledger["codex_calls_dispatched"] += 1
            ledger["updated_at"] = _now()
            _replace_owned_json(ledger_file, ledger)
            result = _run_one_audit_call(root=root, batch=batch, attempt_number=attempt, codex_binary="codex", timeout_seconds=600)
            batch["attempts"].append(result)
            ledger["tokens_used"] += int((result.get("usage") or {}).get("total_tokens") or 0)
            if result.get("throttled"):
                ledger["codex_calls_failed"] += 1
                ledger.update(state="stopped_provider_throttling", updated_at=_now())
                _replace_owned_json(ledger_file, ledger)
                raise ResumeBenchmarkError("audit provider throttling detected")
            if result.get("schema_valid"):
                batch["state"] = "complete"
                ledger["codex_calls_succeeded"] += 1
                _replace_owned_json(ledger_file, ledger)
                break
            batch["state"] = "retry_pending"
            ledger["codex_calls_failed"] += 1
            _replace_owned_json(ledger_file, ledger)
        if batch.get("state") != "complete":
            ledger.update(state="stopped_batch_failure", updated_at=_now())
            _replace_owned_json(ledger_file, ledger)
            raise ResumeBenchmarkError(f"audit batch failed twice: {batch['batch_id']}")
    ledger.update(state="audit_complete_pending_report", updated_at=_now())
    _replace_owned_json(ledger_file, ledger)
    return ledger


def finalize_windowed_audit(*, ledger_path: Path, output_path: Path) -> dict[str, Any]:
    ledger_file = _require_shadow(ledger_path)
    output_file = _require_shadow(output_path)
    ledger = _read_object(ledger_file)
    if ledger.get("state") != "audit_complete_pending_report":
        raise ResumeBenchmarkError("audit is incomplete")
    cases, evaluations, walls = [], [], []
    for batch in ledger["batch_plan"]:
        success = next(a for a in reversed(batch["attempts"]) if a.get("schema_valid"))
        packet = _read_object(Path(batch["packet_path"]))
        output = _read_object(Path(success["output_path"]))
        cases.extend(packet["cases"])
        evaluations.extend(output["evaluations"])
        walls.append(float(success["elapsed_seconds"]))
    score = score_source_audit({"instructions": AUDIT_INSTRUCTIONS, "cases": cases}, {"evaluations": evaluations})
    if int(score["candidate_count"]) != int(ledger["candidate_count"]):
        raise ResumeBenchmarkError("audit candidate coverage mismatch")
    report = {
        "schema_version": "pif_glm52_windowed_random_source_audit_v1", "created_at": _now(), "fold": "DEVELOPMENT",
        "sampling_seed": ledger["sampling_seed"], "selected_segment_ids": ledger["selected_segment_ids"], "segments": ledger["segments"], "candidate_count": ledger["candidate_count"],
        "judge_model": ledger["audit_model"], "codex_calls": ledger["codex_calls_dispatched"], "codex_call_ceiling": ledger["codex_call_ceiling"], "tokens_used": ledger["tokens_used"], "successful_call_wall_seconds": _numeric_distribution(walls),
        "score": score,
        "references": {
            "canary": {"candidate_count": 108, "verdict": {"keep": 82, "revise": 21, "drop": 5}, "support": {"supported": 101, "partial": 7, "unsupported": 0}, "distinctness": {"distinct": 103, "fragment": 5, "duplicate": 0}, "signal": {"useful": 102, "low_signal": 6}, "type_fit": {"correct": 92, "neighbor": 16, "wrong": 0}},
            "pre_windowed_phase3": {"candidate_count": 445, "keep_fraction": 0.7708, "supported_fraction": 0.9079, "supported_or_partial_fraction": 0.9978, "useful_fraction": 0.9506, "type_correct_fraction": 0.9303, "distinct": 409, "duplicate": 26, "fragment": 10},
        },
        "reference_set_used_for_candidate_validity": False, "holdout_opened": False, "extractor_gate_initialized": False, "production_database_writes": 0,
        "evaluations": evaluations,
    }
    _write_new_json(output_file, report)
    ledger.update(state="complete", updated_at=_now(), report_path=str(output_file), report_sha256=_sha256_file(output_file))
    _replace_owned_json(ledger_file, ledger)
    return {**report, "path": str(output_file), "file_sha256": _sha256_file(output_file)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    p = sub.add_parser("resume-enrichment"); p.add_argument("--ledger", required=True)
    p = sub.add_parser("finalize-enrichment"); p.add_argument("--ledger", required=True); p.add_argument("--output", required=True)
    p = sub.add_parser("init-audit"); p.add_argument("--manifest", required=True); p.add_argument("--candidate-root", required=True); p.add_argument("--output-root", required=True)
    p = sub.add_parser("run-audit"); p.add_argument("--ledger", required=True)
    p = sub.add_parser("finalize-audit"); p.add_argument("--ledger", required=True); p.add_argument("--output", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.action == "resume-enrichment": result = resume_enrichment(ledger_path=Path(args.ledger))
    elif args.action == "finalize-enrichment": result = finalize_enrichment_benchmark(ledger_path=Path(args.ledger), output_path=Path(args.output))
    elif args.action == "init-audit": result = initialize_windowed_audit(manifest_path=Path(args.manifest), candidate_root=Path(args.candidate_root), output_root=Path(args.output_root))
    elif args.action == "run-audit": result = run_windowed_audit(ledger_path=Path(args.ledger))
    else: result = finalize_windowed_audit(ledger_path=Path(args.ledger), output_path=Path(args.output))
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
