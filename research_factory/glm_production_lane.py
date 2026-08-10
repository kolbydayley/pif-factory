"""GLM full-contract production lane -- E1 conformance probe.

E1 asks one question: can GLM emit a *conformant* ai_discourse_v3_1 record for a
real segment, given the real production prompt?

That was never actually established. The run that closed the GLM lane was n=3:
one segment returned a single event, one timed out at 600s, and one produced
28,019 output tokens that were discarded whole because a single
``reported_actor.actor_type`` enum failed and ``_validate_schema`` validates the
entire object. So three changes separate this probe from that one:

* timeout 900s, not 600s (the prior timeout killed a stream mid-flight);
* agent ``steps`` raised, so a long structured answer is not truncated;
* ``salvage_structural=True``, so one bad enum drops one event instead of
  destroying the segment.

Shadow-only. Writes filesystem artifacts, never the ``labels`` table.
"""

from __future__ import annotations

import json
import statistics
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import glm_candidate_lane as lane
from .glm_workhorse import DEFAULT_MODEL
from .labels import (
    ValidationError,
    repair_label_output_for_submission,
    validate_label_output,
)

__all__ = ["ProductionLaneError", "select_probe_entries", "run_conformance_probe"]

PROBE_SCHEMA_VERSION = "pif_glm52_full_contract_conformance_probe_v1"
PROBE_TIMEOUT_SECONDS = 900
PROBE_AGENT_STEPS = 40
DEFAULT_OPENCODE_BINARY = "/opt/homebrew/bin/opencode"


class ProductionLaneError(RuntimeError):
    pass


def _probe_opencode_config(base: Mapping[str, Any]) -> dict[str, Any]:
    """The aligned config with a step budget that can carry a long answer.

    ``steps: 8`` is fine for a narrow 3-field candidate list. A full v3.1 record
    carries 29 required fields per event across as many as 40 events, so the
    prior configuration could exhaust its step budget mid-structure and return a
    truncated object indistinguishable from a genuinely short answer.

    Takes the base config as an argument rather than fetching it, so the caller
    can pass the *original* function's output while the module attribute is
    patched -- fetching it here would recurse into the patch.
    """
    config = json.loads(json.dumps(base))
    for spec in (config.get("agent") or {}).values():
        if isinstance(spec, dict):
            spec["steps"] = PROBE_AGENT_STEPS
    return config


def select_probe_entries(
    manifest_path: Path, *, count: int = 8, fold: str = "DEVELOPMENT"
) -> list[Mapping[str, Any]]:
    """Pick ``count`` entries spread across the fold's event-count quartiles.

    Density stratification matters here: conformance failures are expected to be
    concentrated in dense segments, because they demand the longest output. A
    sample skewed sparse would understate the problem.
    """
    if fold != "DEVELOPMENT":
        raise ProductionLaneError(
            "E1 probes DEVELOPMENT only; the holdout gets exactly one attempt, at E7"
        )
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    content = manifest.get("content") or manifest
    entries = list(((content.get("folds") or {}).get(fold) or {}).get("entries") or [])
    if not entries:
        raise ProductionLaneError(f"manifest fold {fold} has no entries")
    ordered = sorted(entries, key=lambda row: (int(row.get("event_count") or 0), str(row["segment_id"])))
    if count >= len(ordered):
        return ordered
    # Even positional spread over the density-sorted fold.
    step = (len(ordered) - 1) / (count - 1) if count > 1 else 0
    picked_indices = sorted({int(round(index * step)) for index in range(count)})
    return [ordered[index] for index in picked_indices]


def _assert_no_production_writes(database: Path) -> None:
    """Fail if any GLM-produced row has reached the labels table.

    Scoped to the GLM model deliberately. The table legitimately holds ~1,553
    historical rows from gpt-5.4 and local-draft on older packs; treating those
    as contamination makes the guard fire on normal state, and a guard that
    always fires is a guard nobody keeps.
    """
    connection = lane._ro_connection(Path(database))
    try:
        row = connection.execute(
            "SELECT count(*) AS n FROM labels WHERE model = ?", (DEFAULT_MODEL,)
        ).fetchone()
    finally:
        connection.close()
    if int(row["n"]):
        raise ProductionLaneError(
            f"labels table already holds {row['n']} {DEFAULT_MODEL} rows; "
            "shadow lanes must never publish"
        )


def run_conformance_probe(
    *,
    manifest_path: Path,
    output_root: Path,
    database: Path,
    count: int = 8,
    call_ceiling: int = 24,
    timeout_seconds: int = PROBE_TIMEOUT_SECONDS,
    opencode_binary: str = DEFAULT_OPENCODE_BINARY,
) -> dict[str, Any]:
    if count > call_ceiling:
        raise ProductionLaneError(f"count {count} exceeds declared call ceiling {call_ceiling}")
    _assert_no_production_writes(Path(database))

    output_root = Path(output_root)
    if output_root.exists():
        raise ProductionLaneError(f"probe root already exists: {output_root}")
    for child in (
        "prompts", "prompt-receipts", "transport", "raw-events", "raw-answers",
        "stderr", "scratch", "worker-state", "validated",
    ):
        (output_root / child).mkdir(parents=True)

    entries = select_probe_entries(Path(manifest_path), count=count)
    connection = lane._ro_connection(Path(database))

    original_config = lane._aligned_opencode_config
    lane._aligned_opencode_config = lambda: _probe_opencode_config(original_config())  # type: ignore[assignment]
    results: list[dict[str, Any]] = []
    try:
        for entry in entries:
            segment_id = str(entry["segment_id"])
            prompt = lane._aligned_context_prompt(connection, entry)
            started = time.monotonic()
            attempt = lane._aligned_run_one(
                entry=entry,
                prompt=prompt,
                output_root=output_root,
                timeout_seconds=timeout_seconds,
                opencode_binary=opencode_binary,
            )
            record = _evaluate_attempt(
                entry=entry,
                attempt=attempt,
                output_root=output_root,
                wall_seconds=round(time.monotonic() - started, 3),
            )
            results.append(record)
    finally:
        lane._aligned_opencode_config = original_config  # type: ignore[assignment]
        connection.close()

    return _summarise(results, output_root=output_root, entries=entries)


def _evaluate_attempt(
    *, entry: Mapping[str, Any], attempt: Mapping[str, Any], output_root: Path, wall_seconds: float
) -> dict[str, Any]:
    segment_id = str(entry["segment_id"])
    segment_text = Path(str(entry["segment_text_path"])).read_text(encoding="utf-8")
    record: dict[str, Any] = {
        "segment_id": segment_id,
        "gold_event_count": int(entry.get("event_count") or 0),
        "wall_seconds": wall_seconds,
        "timed_out": bool(attempt.get("timed_out")),
        "exit_code": attempt.get("exit_code"),
        "stream_event_count": attempt.get("stream_event_count"),
        "output_tokens": ((attempt.get("usage") or {}) or {}).get("output_tokens"),
        "total_tokens": ((attempt.get("usage") or {}) or {}).get("total_tokens"),
        "schema_valid": False,
        "raw_contract_valid": False,
        "usable_after_production_repair": False,
        "structural_drops": 0,
        "metric_quarantines": 0,
        "event_count": 0,
        "failure": None,
    }

    answer_path = output_root / "raw-answers" / f"{segment_id}.private.txt"
    raw = answer_path.read_text(encoding="utf-8") if answer_path.is_file() else ""
    record["raw_answer_bytes"] = len(raw.encode("utf-8"))
    if not raw.strip():
        record["failure"] = "empty_answer"
        return record

    try:
        # _decode_answer already parses, and returns (payload, fence_removed).
        payload, fence_removed = lane._decode_answer(raw)
        record["markdown_fence_removed"] = bool(fence_removed)
    except Exception as error:
        record["failure"] = f"json_decode: {str(error)[:120]}"
        return record
    if not isinstance(payload, dict):
        record["failure"] = "answer_is_not_an_object"
        return record
    record["extraction_status"] = payload.get("extraction_status")
    record["raw_event_count"] = len(payload.get("discourse_events") or [])

    try:
        validate_label_output(lane.LABEL_PACK, payload, segment_text=segment_text)
        record["schema_valid"] = True
        record["raw_contract_valid"] = True
    except ValidationError as error:
        record["raw_validation_error"] = str(error)[:200]

    # The decisive measurement: does it survive the repair chain production runs,
    # with per-event structural salvage enabled?
    repaired = json.loads(json.dumps(payload))
    quarantines: list[dict[str, Any]] = []
    drops: list[dict[str, Any]] = []
    try:
        repair_label_output_for_submission(
            lane.LABEL_PACK,
            repaired,
            segment_text=segment_text,
            metric_quarantines=quarantines,
            salvage_structural=True,
            structural_drops=drops,
        )
        validate_label_output(lane.LABEL_PACK, repaired, segment_text=segment_text)
        record["usable_after_production_repair"] = True
        record["event_count"] = len(repaired.get("discourse_events") or [])
        (output_root / "validated" / f"{segment_id}.private.json").write_text(
            json.dumps(repaired, indent=2, sort_keys=True), encoding="utf-8"
        )
    except ValidationError as error:
        record["failure"] = f"unusable_after_repair: {str(error)[:160]}"
    record["structural_drops"] = len(drops)
    record["metric_quarantines"] = len(quarantines)
    if drops:
        record["structural_drop_rules"] = [d["failed_rule"][:140] for d in drops[:5]]
    return record


def _summarise(
    results: Sequence[Mapping[str, Any]], *, output_root: Path, entries: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    usable = [r for r in results if r["usable_after_production_repair"]]
    walls = [float(r["wall_seconds"]) for r in results]
    out_tokens = [int(r["output_tokens"]) for r in results if r.get("output_tokens")]
    gold = sum(int(r["gold_event_count"]) for r in results)
    got = sum(int(r["event_count"]) for r in usable)

    def _p(values: Sequence[float], q: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        index = min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))
        return round(ordered[index], 3)

    summary = {
        "schema_version": PROBE_SCHEMA_VERSION,
        "model": DEFAULT_MODEL,
        "provider_lane": "zai_coding_plan",
        "calls": len(results),
        "usable_after_production_repair": len(usable),
        "usable_fraction": round(len(usable) / len(results), 4) if results else None,
        "schema_valid": sum(1 for r in results if r["schema_valid"]),
        "raw_contract_valid": sum(1 for r in results if r["raw_contract_valid"]),
        "timeouts": sum(1 for r in results if r["timed_out"]),
        "median_wall_seconds": round(statistics.median(walls), 3) if walls else None,
        "p95_wall_seconds": _p(walls, 0.95),
        "median_output_tokens": round(statistics.median(out_tokens), 1) if out_tokens else None,
        "p95_output_tokens": _p([float(t) for t in out_tokens], 0.95),
        "structural_drops": sum(int(r["structural_drops"]) for r in results),
        "metric_quarantines": sum(int(r["metric_quarantines"]) for r in results),
        "gold_events_on_probed_segments": gold,
        "glm_events_on_usable_segments": got,
        "production_database_writes": 0,
        "holdout_opened": False,
        "results": list(results),
    }
    # Pre-registered E1 decision rule.
    median_wall = summary["median_wall_seconds"] or 0.0
    if len(usable) >= 6 and median_wall < 300:
        verdict = "advance"
    elif len(usable) < 4 or median_wall > 600:
        verdict = "kill_unwindowed"
    else:
        verdict = "inconclusive_proceed_to_windowed"
    summary["verdict"] = verdict
    (output_root / "conformance-probe-report.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    return summary
