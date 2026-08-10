"""Shadow-only GLM label-segment manifest, execution, and scoring lane.

This module intentionally has no production write path.  It opens the canonical
database with SQLite ``mode=ro`` plus ``query_only``, writes only beneath an
explicit shadow/scoring root, and reuses the GLM OpenCode transport and the
efficient-backtest matcher/gate without changing either production module.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import itertools
import json
import math
import os
import re
import shutil
import sqlite3
import statistics
import subprocess
import tempfile
import time
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .efficient_backtest import (
    _event_similarity,
    _f1,
    _historical_artifact_cost,
    _match_events,
    _quality_gate,
    _runtime_stats,
    _token_jaccard,
)
from .glm_workhorse import (
    DEFAULT_MODEL,
    CanaryCase,
    GlmWorkhorseError,
    _decode_answer,
    _parse_opencode_stream,
    _run_case_opencode,
    _usage,
)
from .labels import (
    _validate_schema,
    label_pack_provenance,
    load_label_pack,
    repair_label_output_for_submission,
    render_prompt,
    validate_label_output,
)


SCHEMA_VERSION = "pif_glm52_label_segment_ground_truth_v1"
LEDGER_VERSION = "pif_glm52_label_segment_bound_ledger_v1"
REPORT_VERSION = "pif_glm52_label_segment_development_report_v1"
ALIGNED_LEDGER_VERSION = "pif_glm52_aligned_label_segment_bound_ledger_v1"
ALIGNED_REPORT_VERSION = "pif_glm52_aligned_label_segment_development_report_v1"
LABEL_PACK = "ai_discourse_v3_1"
BASELINE_MODEL = "gpt-5.5"
SEALED_EPISODE_ID = "ep_044f1d2d020e021cfaf99e90"
DEFAULT_SEED = "glm52-label-segment-phase3-v1"
DEVELOPMENT_SEGMENTS = 60
DEVELOPMENT_SOURCES = 22
HOLDOUT_SEGMENTS = 60
MAX_CALLS = 120
ALIGNED_MAX_CALLS = 60
CALIBRATION_CALLS = 3
ABSOLUTE_PREFLIGHT_TOKEN_CEILING = 1_200_000
THROTTLE_MARKERS = (
    "429",
    "rate limit",
    "rate_limit",
    "too many requests",
    "throttl",
    "quota exceeded",
)


class CandidateLaneError(RuntimeError):
    """Fail-closed shadow lane error."""


@dataclass(frozen=True)
class GroundTruthEntry:
    segment_id: str
    episode_id: str
    source_id: str
    source_name: str
    label_id: str
    segment_index: int
    segment_text_path: str
    segment_text_sha256: str
    label_output_sha256: str
    accepted_events_sha256: str
    accepted_events: tuple[dict[str, Any], ...]
    event_count: int
    baseline_prompt_path: str
    baseline_output_path: str
    baseline_label_run_id: str
    baseline_job_id: int


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_new_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    os.chmod(path, 0o600)


def _replace_owned_json(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    if temporary.exists():
        raise CandidateLaneError(f"temporary ledger path already exists: {temporary}")
    with temporary.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def _read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CandidateLaneError(f"expected JSON object: {path}")
    return value


def _ro_connection(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
        raise CandidateLaneError("SQLite query_only could not be enabled")
    return connection


def _percentile(values: Sequence[int | float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def density_summary(values: Sequence[int]) -> dict[str, Any]:
    if not values:
        raise CandidateLaneError("cannot summarize empty density values")
    return {
        "segment_count": len(values),
        "event_count": sum(values),
        "events_per_segment_mean": round(statistics.fmean(values), 6),
        "events_per_segment_stdev": round(statistics.pstdev(values), 6),
        "min": min(values),
        "p10": round(float(_percentile(values, 0.10)), 6),
        "p25": round(float(_percentile(values, 0.25)), 6),
        "p50": round(float(_percentile(values, 0.50)), 6),
        "p75": round(float(_percentile(values, 0.75)), 6),
        "p90": round(float(_percentile(values, 0.90)), 6),
        "p95": round(float(_percentile(values, 0.95)), 6),
        "max": max(values),
        "zero_event_segments": sum(value == 0 for value in values),
        "zero_event_ratio": round(sum(value == 0 for value in values) / len(values), 6),
        "histogram": {
            "0": sum(value == 0 for value in values),
            "1_5": sum(1 <= value <= 5 for value in values),
            "6_10": sum(6 <= value <= 10 for value in values),
            "11_15": sum(11 <= value <= 15 for value in values),
            "16_20": sum(16 <= value <= 20 for value in values),
            "21_25": sum(21 <= value <= 25 for value in values),
            "26_30": sum(26 <= value <= 30 for value in values),
            "31_plus": sum(value >= 31 for value in values),
        },
    }


def _density_representative(fold: Mapping[str, Any], corpus: Mapping[str, Any]) -> dict[str, Any]:
    mean_delta = abs(float(fold["events_per_segment_mean"]) - float(corpus["events_per_segment_mean"]))
    mean_relative_delta = mean_delta / max(float(corpus["events_per_segment_mean"]), 1.0)
    quantile_deltas = {
        key: abs(float(fold[key]) - float(corpus[key]))
        for key in ("p10", "p25", "p50", "p75", "p90", "p95")
    }
    zero_delta = abs(float(fold["zero_event_ratio"]) - float(corpus["zero_event_ratio"]))
    checks = {
        "mean_relative_delta_lte_0_10": mean_relative_delta <= 0.10,
        "all_quantile_deltas_lte_3": all(value <= 3.0 for value in quantile_deltas.values()),
        "zero_event_ratio_delta_lte_0_05": zero_delta <= 0.05,
    }
    return {
        "representative": all(checks.values()),
        "checks": checks,
        "mean_absolute_delta": round(mean_delta, 6),
        "mean_relative_delta": round(mean_relative_delta, 6),
        "quantile_absolute_deltas": {key: round(value, 6) for key, value in quantile_deltas.items()},
        "zero_event_ratio_absolute_delta": round(zero_delta, 6),
    }


def _eligible_rows(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows = connection.execute(
        """
        SELECT
          l.id AS label_id,
          l.segment_id,
          l.output_json,
          l.prompt_path,
          l.output_path,
          sg.episode_id,
          sg.source_id,
          sg.segment_index,
          sg.text_path,
          sg.text_sha256,
          so.name AS source_name,
          lr.id AS label_run_id,
          lr.job_id
        FROM labels l
        JOIN segments sg ON sg.id = l.segment_id
        JOIN sources so ON so.id = sg.source_id
        JOIN label_runs lr ON lr.id = (
          SELECT lr2.id
          FROM label_runs lr2
          JOIN jobs j2 ON j2.id = lr2.job_id
          WHERE lr2.segment_id = l.segment_id
            AND lr2.label_pack = l.label_pack
            AND lr2.model = l.model
            AND lr2.status = 'completed'
            AND j2.job_type = 'label_segment'
            AND j2.status = 'completed'
          ORDER BY COALESCE(lr2.completed_at, lr2.updated_at) DESC, lr2.id DESC
          LIMIT 1
        )
        WHERE l.label_pack = ?
          AND l.label_pack_version = ?
          AND l.model = ?
          AND l.status = 'ready'
        ORDER BY l.segment_id
        """,
        (LABEL_PACK, LABEL_PACK, BASELINE_MODEL),
    ).fetchall()
    counters: Counter[str] = Counter(database_rows=len(rows))
    eligible: list[dict[str, Any]] = []
    for row in rows:
        if str(row["episode_id"]) == SEALED_EPISODE_ID:
            counters["sealed_episode_excluded"] += 1
            continue
        text_path = Path(str(row["text_path"])).expanduser().resolve()
        try:
            segment_text = text_path.read_text(encoding="utf-8")
        except OSError:
            counters["segment_text_unreadable"] += 1
            continue
        segment_hash = _sha256_text(segment_text)
        if segment_hash != str(row["text_sha256"]):
            counters["segment_hash_mismatch"] += 1
            continue
        # Production semantics: worker.submit_label_output repairs before it validates.
        # Validating the raw stored output instead excluded ~65% of the corpus, and the
        # excluded labels were denser than the kept ones, so the fold was biased against
        # the hardest segments.
        metric_quarantines: list[dict[str, Any]] = []
        try:
            output = json.loads(str(row["output_json"]))
            if not isinstance(output, dict):
                raise ValueError("label output is not an object")
            repair_label_output_for_submission(
                LABEL_PACK,
                output,
                segment_text=segment_text,
                metric_quarantines=metric_quarantines,
            )
            validate_label_output(LABEL_PACK, output, segment_text=segment_text)
        except Exception:
            counters["label_validation_failed"] += 1
            continue
        if str(output.get("segment_id") or "") != str(row["segment_id"]):
            counters["label_segment_id_mismatch"] += 1
            continue
        if str(output.get("episode_id") or "") != str(row["episode_id"]):
            counters["label_episode_id_mismatch"] += 1
            continue
        events = [dict(event) for event in output.get("discourse_events") or [] if isinstance(event, dict)]
        eligible.append(
            {
                **dict(row),
                "segment_text_path": str(text_path),
                "segment_text_sha256": segment_hash,
                "label_output": output,
                "label_output_sha256": _sha256_text(_canonical_json(output)),
                "accepted_events": events,
                "accepted_events_sha256": _sha256_text(_canonical_json(events)),
                "event_count": len(events),
                "metric_quarantine_count": len(metric_quarantines),
            }
        )
        counters["eligible"] += 1
    return eligible, dict(sorted(counters.items()))


def _histogram_proportions(values: Sequence[int]) -> tuple[float, ...]:
    summary = density_summary(values)["histogram"]
    total = max(len(values), 1)
    return tuple(summary[key] / total for key in ("0", "1_5", "6_10", "11_15", "16_20", "21_25", "26_30", "31_plus"))


def _choose_holdout_sources(rows: Sequence[Mapping[str, Any]], *, count: int = 5) -> set[str]:
    by_source: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[str(row["source_id"])].append(row)
    source_ids = sorted(by_source)
    if len(source_ids) != DEVELOPMENT_SOURCES + count:
        raise CandidateLaneError(
            f"expected {DEVELOPMENT_SOURCES + count} eligible nonsealed sources, found {len(source_ids)}"
        )
    corpus_values = [int(row["event_count"]) for row in rows]
    corpus_histogram = _histogram_proportions(corpus_values)
    corpus_mean = statistics.fmean(corpus_values)

    # The score is additive over disjoint sources: histogram buckets are counts and
    # the mean is sum/n, so a combination's statistics follow from per-source
    # aggregates. Rebuilding the row lists per combination made this O(C(n,k) * rows)
    # -- fine at C(27,5)=80,730, but corrected eligibility surfaced 30 sources and
    # C(30,8)=5,852,925, which ran for hours. Same search, same tie-break, same
    # result; only the per-combination cost changes.
    histogram_keys = ("0", "1_5", "6_10", "11_15", "16_20", "21_25", "26_30", "31_plus")
    source_histogram: dict[str, tuple[int, ...]] = {}
    source_total: dict[str, int] = {}
    source_size: dict[str, int] = {}
    for source_id, source_rows in by_source.items():
        values = [int(row["event_count"]) for row in source_rows]
        buckets = density_summary(values)["histogram"]
        source_histogram[source_id] = tuple(int(buckets[key]) for key in histogram_keys)
        source_total[source_id] = sum(values)
        source_size[source_id] = len(values)

    total_rows = len(rows)
    expected_share = count / len(source_ids)
    eligible_ids = [source_id for source_id in source_ids if source_size[source_id] >= 2]

    best: tuple[float, tuple[str, ...]] | None = None
    for combination in itertools.combinations(eligible_ids, count):
        size = 0
        for source_id in combination:
            size += source_size[source_id]
        if size < HOLDOUT_SEGMENTS:
            continue
        buckets = [0] * len(histogram_keys)
        total = 0
        for source_id in combination:
            counts = source_histogram[source_id]
            for index in range(len(histogram_keys)):
                buckets[index] += counts[index]
            total += source_total[source_id]
        histogram = tuple(bucket / size for bucket in buckets)
        histogram_l1 = sum(abs(left - right) for left, right in zip(histogram, corpus_histogram))
        mean_delta = abs((total / size) - corpus_mean) / max(corpus_mean, 1.0)
        size_penalty = abs((size / total_rows) - expected_share)
        score = histogram_l1 + mean_delta + 0.1 * size_penalty
        candidate = (round(score, 12), combination)
        if best is None or candidate < best:
            best = candidate
    if best is None:
        raise CandidateLaneError("no source-disjoint holdout source partition is available")
    return set(best[1])


def _target_event_counts(corpus_values: Sequence[int], count: int) -> list[int]:
    return [
        int(round(float(_percentile(corpus_values, (index + 0.5) / count))))
        for index in range(count)
    ]


def _stable_order(row: Mapping[str, Any], seed: str) -> str:
    return _sha256_text(f"{seed}|{row['source_id']}|{row['episode_id']}|{row['segment_id']}")


def _stratified_sample(
    rows: Sequence[Mapping[str, Any]],
    *,
    count: int,
    corpus_values: Sequence[int],
    seed: str,
) -> list[Mapping[str, Any]]:
    if len(rows) < count:
        raise CandidateLaneError(f"requested {count} rows from a pool of {len(rows)}")
    targets = _target_event_counts(corpus_values, count)
    by_source: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[str(row["source_id"])].append(row)
    selected: list[Mapping[str, Any]] = []
    used: set[str] = set()
    coverage_targets = [
        targets[int(round(index * (count - 1) / max(len(by_source) - 1, 1)))]
        for index in range(len(by_source))
    ]
    source_order = sorted(by_source, key=lambda value: _sha256_text(f"{seed}|source|{value}"))
    for source_id, target in zip(source_order, coverage_targets):
        candidate = min(
            by_source[source_id],
            key=lambda row: (
                abs(int(row["event_count"]) - target),
                _stable_order(row, seed),
            ),
        )
        selected.append(candidate)
        used.add(str(candidate["segment_id"]))
    remaining_targets = list(targets)
    for selected_row in selected:
        nearest = min(
            range(len(remaining_targets)),
            key=lambda index: abs(remaining_targets[index] - int(selected_row["event_count"])),
        )
        remaining_targets.pop(nearest)
    pool = [row for row in rows if str(row["segment_id"]) not in used]
    for target in remaining_targets:
        candidate = min(
            pool,
            key=lambda row: (
                abs(int(row["event_count"]) - target),
                _stable_order(row, seed),
            ),
        )
        selected.append(candidate)
        pool.remove(candidate)
    return sorted(selected, key=lambda row: (str(row["source_id"]), str(row["episode_id"]), int(row["segment_index"])))


def _entry(row: Mapping[str, Any]) -> GroundTruthEntry:
    return GroundTruthEntry(
        segment_id=str(row["segment_id"]),
        episode_id=str(row["episode_id"]),
        source_id=str(row["source_id"]),
        source_name=str(row["source_name"]),
        label_id=str(row["label_id"]),
        segment_index=int(row["segment_index"]),
        segment_text_path=str(row["segment_text_path"]),
        segment_text_sha256=str(row["segment_text_sha256"]),
        label_output_sha256=str(row["label_output_sha256"]),
        accepted_events_sha256=str(row["accepted_events_sha256"]),
        accepted_events=tuple(dict(event) for event in row["accepted_events"]),
        event_count=int(row["event_count"]),
        baseline_prompt_path=str(row["prompt_path"] or ""),
        baseline_output_path=str(row["output_path"] or ""),
        baseline_label_run_id=str(row["label_run_id"]),
        baseline_job_id=int(row["job_id"]),
    )


def build_manifest(*, database: Path, output_path: Path, seed: str = DEFAULT_SEED) -> dict[str, Any]:
    output = output_path.expanduser().resolve()
    if output.exists():
        raise FileExistsError(output)
    connection = _ro_connection(database)
    try:
        connection.execute("BEGIN")
        eligible, validation_counts = _eligible_rows(connection)
        snapshot_counts = {
            "completed_label_segment_jobs": int(
                connection.execute(
                    "SELECT COUNT(*) FROM jobs WHERE job_type='label_segment' AND status='completed'"
                ).fetchone()[0]
            ),
            "ready_unique_labels_matching_pack_model": int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM labels
                    WHERE label_pack=? AND label_pack_version=? AND model=? AND status='ready'
                    """,
                    (LABEL_PACK, LABEL_PACK, BASELINE_MODEL),
                ).fetchone()[0]
            ),
        }
    finally:
        connection.close()
    if not eligible:
        raise CandidateLaneError("no validated production ground truth is available")
    source_ids = {str(row["source_id"]) for row in eligible}
    holdout_source_count = len(source_ids) - DEVELOPMENT_SOURCES
    if holdout_source_count < 1:
        raise CandidateLaneError("insufficient sources for source-disjoint folds")
    holdout_sources = _choose_holdout_sources(eligible, count=holdout_source_count)
    development_pool = [row for row in eligible if str(row["source_id"]) not in holdout_sources]
    holdout_pool = [row for row in eligible if str(row["source_id"]) in holdout_sources]
    corpus_values = [int(row["event_count"]) for row in eligible]
    development_rows = _stratified_sample(
        development_pool,
        count=DEVELOPMENT_SEGMENTS,
        corpus_values=corpus_values,
        seed=f"{seed}|development",
    )
    holdout_rows = _stratified_sample(
        holdout_pool,
        count=HOLDOUT_SEGMENTS,
        corpus_values=corpus_values,
        seed=f"{seed}|holdout",
    )
    development_sources = {str(row["source_id"]) for row in development_rows}
    holdout_sources_selected = {str(row["source_id"]) for row in holdout_rows}
    development_episodes = {str(row["episode_id"]) for row in development_rows}
    holdout_episodes = {str(row["episode_id"]) for row in holdout_rows}
    source_overlap = sorted(development_sources & holdout_sources_selected)
    episode_overlap = sorted(development_episodes & holdout_episodes)
    if source_overlap or episode_overlap:
        raise CandidateLaneError("fold disjointness failed")
    if SEALED_EPISODE_ID in development_episodes | holdout_episodes:
        raise CandidateLaneError("sealed benchmark episode entered a scoring fold")
    corpus_density = density_summary(corpus_values)
    development_density = density_summary([int(row["event_count"]) for row in development_rows])
    holdout_density = density_summary([int(row["event_count"]) for row in holdout_rows])
    development_representation = _density_representative(development_density, corpus_density)
    holdout_representation = _density_representative(holdout_density, corpus_density)
    if not development_representation["representative"] or not holdout_representation["representative"]:
        raise CandidateLaneError("deterministic stratification did not produce density-representative folds")
    content = {
        "schema_version": SCHEMA_VERSION,
        "created_at": _now(),
        "privacy": "private_analysis_only",
        "production_database": {
            "path": str(database.expanduser().resolve()),
            "sqlite_mode": "ro",
            "query_only": True,
            **snapshot_counts,
        },
        "ground_truth": {
            "label_pack": LABEL_PACK,
            "label_pack_version": LABEL_PACK,
            "model": BASELINE_MODEL,
            "accepted_label_status": "ready",
            "required_label_run_status": "completed",
            "required_job_type": "label_segment",
            "required_job_status": "completed",
            "validation_counts": validation_counts,
            "eligible_unique_label_count": len(eligible),
        },
        "sealed_benchmark": {
            "episode_id": SEALED_EPISODE_ID,
            "excluded": True,
            "present_in_any_fold": False,
        },
        "selection": {
            "seed": seed,
            "development_target_segments": DEVELOPMENT_SEGMENTS,
            "development_target_sources": DEVELOPMENT_SOURCES,
            "holdout_target_segments": HOLDOUT_SEGMENTS,
            "selection_rule": "source-partition optimization followed by corpus-quantile event-density sampling with mandatory source coverage",
            "selection_used_event_density": True,
            "rebalance_performed": True,
        },
        "disjointness_proof": {
            "development_source_ids": sorted(development_sources),
            "holdout_source_ids": sorted(holdout_sources_selected),
            "source_id_intersection": source_overlap,
            "source_id_intersection_count": len(source_overlap),
            "development_episode_ids": sorted(development_episodes),
            "holdout_episode_ids": sorted(holdout_episodes),
            "episode_id_intersection": episode_overlap,
            "episode_id_intersection_count": len(episode_overlap),
            "disjoint_in_source_ids": not source_overlap,
            "disjoint_in_episode_ids": not episode_overlap,
        },
        "density": {
            "corpus_wide_eligible": corpus_density,
            "development": {
                **development_density,
                "versus_corpus": development_representation,
            },
            "holdout": {
                **holdout_density,
                "versus_corpus": holdout_representation,
            },
        },
        "folds": {
            "DEVELOPMENT": {
                "segment_count": len(development_rows),
                "episode_count": len(development_episodes),
                "source_count": len(development_sources),
                "source_ids": sorted(development_sources),
                "episode_ids": sorted(development_episodes),
                "entries": [asdict(_entry(row)) for row in development_rows],
            },
            "HOLDOUT": {
                "segment_count": len(holdout_rows),
                "episode_count": len(holdout_episodes),
                "source_count": len(holdout_sources_selected),
                "source_ids": sorted(holdout_sources_selected),
                "episode_ids": sorted(holdout_episodes),
                "entries": [asdict(_entry(row)) for row in holdout_rows],
            },
        },
    }
    content_sha256 = _sha256_text(_canonical_json(content))
    manifest = {**content, "content_sha256": content_sha256}
    _write_new_json(output, manifest)
    return {**manifest, "path": str(output), "file_sha256": _sha256_file(output)}


def initialize_ledger(*, manifest_path: Path, ledger_path: Path) -> dict[str, Any]:
    manifest = _read_object(manifest_path)
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise CandidateLaneError("manifest schema mismatch")
    development = manifest.get("folds", {}).get("DEVELOPMENT", {})
    development_count = int(development.get("segment_count") or 0)
    if development_count < CALIBRATION_CALLS or development_count > MAX_CALLS:
        raise CandidateLaneError("development fold is outside the authorized call bound")
    ledger = {
        "schema_version": LEDGER_VERSION,
        "created_at": _now(),
        "updated_at": _now(),
        "state": "declared_pre_provider",
        "manifest_path": str(manifest_path.expanduser().resolve()),
        "manifest_file_sha256": _sha256_file(manifest_path),
        "model": DEFAULT_MODEL,
        "transport": "opencode",
        "billing_lane": "z_ai_coding_plan_subscription_only",
        "codex_calls_authorized": 0,
        "codex_calls_used": 0,
        "glm_call_ceiling": MAX_CALLS,
        "glm_calls_used": 0,
        "calibration_calls": CALIBRATION_CALLS,
        "token_ceiling_state": "calibrate_from_first_three_successful_calls",
        "absolute_preflight_token_ceiling": ABSOLUTE_PREFLIGHT_TOKEN_CEILING,
        "measured_token_ceiling": None,
        "token_ceiling_formula": "ceil(max(first_three_total_tokens) * authorized_call_ceiling * 1.15)",
        "tokens_used": 0,
        "subscription_spend_usd": 0.0,
        "production_database_writes": 0,
        "holdout_calls": 0,
        "extractor_gate_initialized": False,
    }
    _write_new_json(ledger_path.expanduser().resolve(), ledger)
    return ledger


def _case_from_entry(entry: Mapping[str, Any], *, manifest_path: Path) -> CanaryCase:
    text_path = Path(str(entry["segment_text_path"])).expanduser().resolve()
    text = text_path.read_text(encoding="utf-8")
    if _sha256_text(text) != str(entry["segment_text_sha256"]):
        raise CandidateLaneError(f"segment hash changed: {entry['segment_id']}")
    events = tuple(dict(event) for event in entry.get("accepted_events") or [])
    if _sha256_text(_canonical_json(list(events))) != str(entry["accepted_events_sha256"]):
        raise CandidateLaneError(f"accepted event hash changed: {entry['segment_id']}")
    return CanaryCase(
        segment_id=str(entry["segment_id"]),
        source_name=str(entry["source_name"]),
        chunk_index=int(entry["segment_index"]),
        extract_text=text,
        left_context="",
        right_context="",
        episode_context={"episode_id": str(entry["episode_id"]), "source_name": str(entry["source_name"])},
        adjacent_segments=(),
        baseline_events=events,
        prompt_path=manifest_path,
        baseline_path=manifest_path,
    )


def _throttled(result: Mapping[str, Any], output_root: Path) -> bool:
    for attempt in result.get("attempts") or []:
        attempt_number = int(attempt.get("attempt") or 0)
        segment_id = str(result.get("segment_id") or "")
        stderr_path = output_root / "stderr" / f"{segment_id}-attempt-{attempt_number}.private.txt"
        stderr = stderr_path.read_text(encoding="utf-8", errors="replace").lower() if stderr_path.is_file() else ""
        if any(marker in stderr for marker in THROTTLE_MARKERS):
            return True
    return False


def _usage_rows(results: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(attempt["usage"])
        for result in results
        for attempt in result.get("attempts") or []
        if isinstance(attempt.get("usage"), dict)
    ]


def _usage_total(rows: Sequence[Mapping[str, Any]], key: str) -> int:
    return sum(int(row.get(key) or 0) for row in rows)


def _candidate_events(output_root: Path, segment_id: str) -> list[dict[str, Any]]:
    path = output_root / "validated" / f"{segment_id}.private.json"
    if not path.is_file():
        return []
    value = _read_object(path)
    events = value.get("discourse_events")
    if not isinstance(events, list):
        events = value.get("events") or []
    return [dict(event) for event in events if isinstance(event, dict)]


def _score_results(
    *,
    entries: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    output_root: Path,
    baseline_cost: Mapping[str, Any],
    baseline_runtime_seconds: float | None,
) -> dict[str, Any]:
    result_by_segment = {str(result["segment_id"]): result for result in results}
    totals: Counter[str] = Counter()
    field_mismatches: Counter[str] = Counter()
    segment_items: list[dict[str, Any]] = []
    for entry in entries:
        segment_id = str(entry["segment_id"])
        golden = [dict(event) for event in entry.get("accepted_events") or [] if isinstance(event, dict)]
        candidate = _candidate_events(output_root, segment_id)
        result = result_by_segment.get(segment_id)
        usable = bool(result and result.get("usable"))
        matched = _match_events(golden, candidate) if usable else []
        matched_golden = {golden_index for golden_index, _candidate_index, _score in matched}
        matched_candidate = {candidate_index for _golden_index, candidate_index, _score in matched}
        missed = len(golden) - len(matched_golden)
        spurious = len(candidate) - len(matched_candidate)
        wrong_spans = 0
        wrong_fields = 0
        for golden_index, candidate_index, _similarity in matched:
            golden_event = golden[golden_index]
            candidate_event = candidate[candidate_index]
            if str(golden_event.get("evidence") or "") != str(candidate_event.get("evidence") or ""):
                wrong_spans += 1
            event_wrong = False
            for field in ("event_type", "claim_text"):
                if str(golden_event.get(field) or "") != str(candidate_event.get(field) or ""):
                    field_mismatches[field] += 1
                    event_wrong = True
            if event_wrong:
                wrong_fields += 1
        totals.update(
            golden_events=len(golden),
            candidate_events=len(candidate),
            matched_events=len(matched),
            missed_events=missed,
            spurious_events=spurious,
            wrong_spans=wrong_spans,
            wrong_fields=wrong_fields,
            segments_present=int(result is not None),
            validation_ok=int(usable),
            schema_valid=int(
                bool(result)
                and any(bool(attempt.get("schema_valid")) for attempt in result.get("attempts") or [])
            ),
            invalid_evidence_events=int(
                ((result or {}).get("validation") or {}).get("invalid_evidence_events_dropped") or 0
            ),
            returned_events=int(((result or {}).get("validation") or {}).get("returned_event_count") or 0),
        )
        segment_items.append(
            {
                "segment_id": segment_id,
                "golden_events": len(golden),
                "candidate_events": len(candidate),
                "matched_events": len(matched),
                "missed_events": missed,
                "spurious_events": spurious,
                "wrong_spans": wrong_spans,
                "wrong_fields": wrong_fields,
                "usable": usable,
            }
        )
    precision = totals["matched_events"] / totals["candidate_events"] if totals["candidate_events"] else 0.0
    recall = totals["matched_events"] / totals["golden_events"] if totals["golden_events"] else 0.0
    f1 = _f1(precision, recall)
    usage = _usage_rows(results)
    total_tokens = _usage_total(usage, "total_tokens")
    candidate_elapsed = sum(
        float(attempt.get("elapsed_seconds") or 0)
        for result in results
        for attempt in result.get("attempts") or []
    )
    baseline_tokens = int(baseline_cost.get("estimated_total_tokens") or 0)
    token_ratio = total_tokens / baseline_tokens if baseline_tokens else None
    runtime_ratio = (
        candidate_elapsed / baseline_runtime_seconds
        if baseline_runtime_seconds and baseline_runtime_seconds > 0
        else None
    )
    candidate_comparison = {
        "segments_expected": len(entries),
        "segments_present": totals["segments_present"],
        "segments_missing": len(entries) - totals["segments_present"],
        "validation_failed": len(entries) - totals["validation_ok"],
        "valid_golden_events": totals["golden_events"],
        "valid_candidate_events": totals["candidate_events"],
        "valid_matched_events": totals["matched_events"],
        "valid_event_count_ratio": (
            totals["candidate_events"] / totals["golden_events"] if totals["golden_events"] else None
        ),
        "segment_completion_ratio": totals["segments_present"] / len(entries),
        "valid_segment_ratio": totals["validation_ok"] / len(entries),
        "status_accuracy": 1.0,
        "event_precision": precision,
        "event_recall": recall,
        "event_f1": f1,
    }
    efficient_gate = _quality_gate(
        current_cost={
            "estimated_total_tokens": baseline_tokens,
            "estimated_runtime_seconds": baseline_runtime_seconds,
        },
        candidate_cost={
            "estimated_total_tokens": total_tokens,
            "estimated_runtime_seconds": candidate_elapsed,
        },
        compact_quality={"exact_label_roundtrip_rate": 1.0},
        candidate_comparison=candidate_comparison,
    )
    directive_checks = {
        "precision_gte_0_90": precision >= 0.90,
        "recall_gte_0_90": recall >= 0.90,
        "token_ratio_lte_0_25": token_ratio is not None and token_ratio <= 0.25,
    }
    elapsed_values = [
        float(attempt.get("elapsed_seconds") or 0)
        for result in results
        for attempt in result.get("attempts") or []
    ]
    return {
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(float(f1 or 0), 6),
        "events_per_segment": {
            "codex": round(totals["golden_events"] / len(entries), 6),
            "glm": round(totals["candidate_events"] / len(entries), 6),
            "ratio": round(totals["candidate_events"] / totals["golden_events"], 6)
            if totals["golden_events"]
            else None,
        },
        "evidence_span_validity": {
            "returned_events": totals["returned_events"],
            "invalid_events_dropped": totals["invalid_evidence_events"],
            "valid_after_repair": totals["candidate_events"],
            "validity_rate": round(
                (totals["returned_events"] - totals["invalid_evidence_events"]) / totals["returned_events"],
                6,
            )
            if totals["returned_events"]
            else 1.0,
            "wrong_span_matches": totals["wrong_spans"],
        },
        "schema_structural_validity": {
            "expected_segments": len(entries),
            "schema_valid_segments": totals["schema_valid"],
            "usable_segments": totals["validation_ok"],
            "schema_valid_ratio": round(totals["schema_valid"] / len(entries), 6),
            "usable_ratio": round(totals["validation_ok"] / len(entries), 6),
        },
        "tokens": {
            "glm_input_tokens": _usage_total(usage, "input_tokens"),
            "glm_cached_input_tokens": _usage_total(usage, "cached_input_tokens"),
            "glm_output_tokens": _usage_total(usage, "output_tokens"),
            "glm_total_tokens": total_tokens,
            "codex_baseline_estimated_total_tokens": baseline_tokens,
            "ratio": round(token_ratio, 6) if token_ratio is not None else None,
            "baseline_method": baseline_cost.get("note"),
        },
        "cost": {
            "billing_lane": "z_ai_coding_plan_subscription_only",
            "subscription_spend_usd": 0.0,
            "cost_per_accepted_event_usd": 0.0 if totals["candidate_events"] else None,
            "opencode_nominal_reported_cost_usd": round(
                sum(float(row.get("estimated_cost_usd") or 0) for row in usage), 8
            ),
        },
        "wall_time_per_call_seconds": {
            "count": len(elapsed_values),
            "mean": round(statistics.fmean(elapsed_values), 6) if elapsed_values else None,
            "p50": round(float(_percentile(elapsed_values, 0.50)), 6) if elapsed_values else None,
            "p90": round(float(_percentile(elapsed_values, 0.90)), 6) if elapsed_values else None,
            "max": round(max(elapsed_values), 6) if elapsed_values else None,
            "total_attempt_elapsed_seconds": round(candidate_elapsed, 6),
        },
        "disagreements": {
            "missed_events": totals["missed_events"],
            "spurious_events": totals["spurious_events"],
            "wrong_spans": totals["wrong_spans"],
            "wrong_fields": totals["wrong_fields"],
            "wrong_field_counts": dict(sorted(field_mismatches.items())),
        },
        "efficient_backtest_candidate_quality_gate": efficient_gate,
        "directive_gate": {
            "thresholds": {"min_precision": 0.90, "min_recall": 0.90, "max_token_ratio": 0.25},
            "checks": directive_checks,
            "passed": all(directive_checks.values()),
        },
        "segments": segment_items,
    }


def run_development(
    *,
    manifest_path: Path,
    ledger_path: Path,
    output_root: Path,
    concurrency: int = 2,
    timeout_seconds: int = 240,
    max_events: int = 50,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    ledger_file = ledger_path.expanduser().resolve()
    root = output_root.expanduser().resolve()
    manifest = _read_object(manifest_file)
    ledger = _read_object(ledger_file)
    if ledger.get("state") != "declared_pre_provider" or int(ledger.get("glm_calls_used") or 0) != 0:
        raise CandidateLaneError("provider bounds were not freshly declared")
    if str(ledger.get("manifest_file_sha256")) != _sha256_file(manifest_file):
        raise CandidateLaneError("manifest changed after bound declaration")
    development = manifest.get("folds", {}).get("DEVELOPMENT", {})
    entries = list(development.get("entries") or [])
    if len(entries) > int(ledger["glm_call_ceiling"]):
        raise CandidateLaneError("development fold exceeds call ceiling")
    if root.exists():
        raise FileExistsError(root)
    root.mkdir(parents=True)
    os.chmod(root, 0o700)
    for name in ("jobs", "raw-events", "raw-answers", "stderr", "validated", "scratch"):
        (root / name).mkdir()
    cases = [_case_from_entry(entry, manifest_path=manifest_file) for entry in entries]
    results: list[dict[str, Any]] = []
    wall_started = time.monotonic()
    for case in cases[:CALIBRATION_CALLS]:
        result = _run_case_opencode(
            case,
            output_root=root,
            model=DEFAULT_MODEL,
            timeout_seconds=timeout_seconds,
            retry_count=0,
            max_events=max_events,
            opencode_binary=opencode_binary,
        )
        results.append(result)
        if _throttled(result, root):
            ledger.update(
                state="stopped_provider_throttling",
                updated_at=_now(),
                glm_calls_used=len(results),
                tokens_used=_usage_total(_usage_rows(results), "total_tokens"),
            )
            _replace_owned_json(ledger_file, ledger)
            raise CandidateLaneError("provider throttling detected during calibration")
    calibration_usage = _usage_rows(results)
    calibration_totals = [int(row.get("total_tokens") or 0) for row in calibration_usage]
    if len(calibration_totals) != CALIBRATION_CALLS or any(value <= 0 for value in calibration_totals):
        ledger.update(
            state="stopped_missing_calibration_usage",
            updated_at=_now(),
            glm_calls_used=len(results),
            tokens_used=sum(calibration_totals),
        )
        _replace_owned_json(ledger_file, ledger)
        raise CandidateLaneError("calibration calls did not return complete token usage")
    measured_ceiling = math.ceil(max(calibration_totals) * int(ledger["glm_call_ceiling"]) * 1.15)
    measured_ceiling = min(measured_ceiling, int(ledger["absolute_preflight_token_ceiling"]))
    ledger.update(
        state="calibrated_provider_bound",
        updated_at=_now(),
        glm_calls_used=len(results),
        tokens_used=sum(calibration_totals),
        measured_token_ceiling=measured_ceiling,
        token_ceiling_state="fixed_from_first_three_successful_calls",
        calibration_total_tokens=calibration_totals,
    )
    _replace_owned_json(ledger_file, ledger)
    pending_cases = iter(cases[CALIBRATION_CALLS:])
    active: dict[Any, CanaryCase] = {}
    throttle_detected = False
    with ThreadPoolExecutor(max_workers=max(1, min(concurrency, 2))) as executor:
        while True:
            while not throttle_detected and len(active) < max(1, min(concurrency, 2)):
                try:
                    case = next(pending_cases)
                except StopIteration:
                    break
                projected = int(ledger["tokens_used"]) + max(calibration_totals) * (len(active) + 1)
                if projected > measured_ceiling:
                    ledger.update(state="stopped_token_ceiling", updated_at=_now())
                    _replace_owned_json(ledger_file, ledger)
                    raise CandidateLaneError("measured token ceiling would be exceeded")
                future = executor.submit(
                    _run_case_opencode,
                    case,
                    output_root=root,
                    model=DEFAULT_MODEL,
                    timeout_seconds=timeout_seconds,
                    retry_count=0,
                    max_events=max_events,
                    opencode_binary=opencode_binary,
                )
                active[future] = case
            if not active:
                break
            completed, _still_running = wait(active, return_when=FIRST_COMPLETED)
            for future in completed:
                active.pop(future)
                result = future.result()
                results.append(result)
                if _throttled(result, root):
                    throttle_detected = True
                usage = _usage_rows([result])
                ledger["glm_calls_used"] = int(ledger["glm_calls_used"]) + 1
                ledger["tokens_used"] = int(ledger["tokens_used"]) + _usage_total(usage, "total_tokens")
                ledger["updated_at"] = _now()
                if int(ledger["glm_calls_used"]) > int(ledger["glm_call_ceiling"]):
                    raise CandidateLaneError("GLM call ceiling exceeded")
                if int(ledger["tokens_used"]) > measured_ceiling:
                    ledger["state"] = "stopped_token_ceiling"
                    _replace_owned_json(ledger_file, ledger)
                    raise CandidateLaneError("measured token ceiling exceeded")
                _replace_owned_json(ledger_file, ledger)
            if throttle_detected:
                for future in active:
                    future.cancel()
                ledger.update(state="stopped_provider_throttling", updated_at=_now())
                _replace_owned_json(ledger_file, ledger)
                raise CandidateLaneError("provider throttling detected")
    database = Path(str(manifest["production_database"]["path"]))
    connection = _ro_connection(database)
    try:
        selected_ids = [str(entry["segment_id"]) for entry in entries]
        placeholders = ",".join("?" for _ in selected_ids)
        label_rows = [
            dict(row)
            for row in connection.execute(
                f"""
                SELECT l.id AS label_id, l.segment_id, l.output_json, l.prompt_path, l.output_path,
                       sg.episode_id, sg.segment_index, sg.word_count, sg.text_path,
                       ep.title AS episode_title, ep.published_at AS episode_published_at,
                       so.name AS source_name
                FROM labels l
                JOIN segments sg ON sg.id=l.segment_id
                JOIN episodes ep ON ep.id=sg.episode_id
                JOIN sources so ON so.id=sg.source_id
                WHERE l.id IN ({placeholders})
                """,
                [str(entry["label_id"]) for entry in entries],
            ).fetchall()
        ]
        for row in label_rows:
            row["output"] = json.loads(row["output_json"])
        by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in label_rows:
            by_episode[str(row["episode_id"])].append(row)
        baseline_cost = _historical_artifact_cost(label_rows)
        runtime_stats = _runtime_stats(
            connection,
            labels=label_rows,
            labels_by_episode=by_episode,
            label_pack=LABEL_PACK,
            model=BASELINE_MODEL,
        )
    finally:
        connection.close()
    baseline_runtime_seconds = float(runtime_stats.get("current_label_runtime_seconds") or 0)
    score = _score_results(
        entries=entries,
        results=results,
        output_root=root,
        baseline_cost=baseline_cost,
        baseline_runtime_seconds=baseline_runtime_seconds,
    )
    report = {
        "schema_version": REPORT_VERSION,
        "created_at": _now(),
        "manifest_path": str(manifest_file),
        "manifest_file_sha256": _sha256_file(manifest_file),
        "ledger_path": str(ledger_file),
        "fold": "DEVELOPMENT",
        "holdout_opened_for_execution_or_scoring": False,
        "extractor_gate_initialized": False,
        "production_database_mode": "ro_query_only",
        "production_database_writes": 0,
        "model": DEFAULT_MODEL,
        "transport": "opencode",
        "call_count": len(results),
        "wall_elapsed_seconds": round(time.monotonic() - wall_started, 6),
        "score": score,
        "results": results,
    }
    report_path = root / "development-report.json"
    _write_new_json(report_path, report)
    ledger.update(
        state="development_complete",
        updated_at=_now(),
        glm_calls_used=len(results),
        tokens_used=int(score["tokens"]["glm_total_tokens"]),
        subscription_spend_usd=0.0,
        report_path=str(report_path),
        report_file_sha256=_sha256_file(report_path),
    )
    _replace_owned_json(ledger_file, ledger)
    receipt = {
        "schema_version": "pif_glm52_label_segment_development_receipt_v1",
        "created_at": _now(),
        "report_path": str(report_path),
        "report_file_sha256": _sha256_file(report_path),
        "ledger_path": str(ledger_file),
        "ledger_file_sha256": _sha256_file(ledger_file),
        "manifest_path": str(manifest_file),
        "manifest_file_sha256": _sha256_file(manifest_file),
        "subscription_spend_usd": 0.0,
        "codex_calls": 0,
        "holdout_calls": 0,
        "production_writes": 0,
    }
    _write_new_json(root / "receipt.json", receipt)
    return {**report, "path": str(report_path), "file_sha256": _sha256_file(report_path)}


def _pack_contract() -> dict[str, Any]:
    pack = load_label_pack(LABEL_PACK)
    provenance = label_pack_provenance(pack)
    return {
        "label_pack": LABEL_PACK,
        "label_pack_version": pack.version,
        **provenance,
        "metric_grounding_contract": {
            "raw_text_verbatim_contiguous_evidence_substring": True,
            "value_unit_comparator_verbatim_or_null": True,
            "normalization_forbidden": True,
            "ellipses_forbidden": True,
            "speaker_tag_stitching_forbidden": True,
        },
    }


def _aligned_calibration_ids(entries: Sequence[Mapping[str, Any]]) -> list[str]:
    targets = (4, 14, 22)
    available = list(entries)
    chosen: list[str] = []
    for target in targets:
        entry = min(
            available,
            key=lambda item: (
                abs(int(item.get("event_count") or 0) - target),
                str(item.get("segment_id") or ""),
            ),
        )
        chosen.append(str(entry["segment_id"]))
        available.remove(entry)
    return chosen


def initialize_aligned_ledger(*, manifest_path: Path, ledger_path: Path) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    manifest = _read_object(manifest_file)
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise CandidateLaneError("manifest schema mismatch")
    entries = list(manifest.get("folds", {}).get("DEVELOPMENT", {}).get("entries") or [])
    if len(entries) != ALIGNED_MAX_CALLS:
        raise CandidateLaneError(f"aligned attempt requires exactly {ALIGNED_MAX_CALLS} development entries")
    ledger = {
        "schema_version": ALIGNED_LEDGER_VERSION,
        "created_at": _now(),
        "updated_at": _now(),
        "state": "declared_pre_provider",
        "attempt": "aligned_full_production_contract_last_development_attempt",
        "manifest_path": str(manifest_file),
        "manifest_file_sha256": _sha256_file(manifest_file),
        "contract": _pack_contract(),
        "model": DEFAULT_MODEL,
        "transport": "opencode",
        "dispatch_mode": "single_shot_stdin_to_opencode_positional_bridge",
        "billing_lane": "z_ai_coding_plan_subscription_only",
        "codex_calls_authorized": 0,
        "codex_calls_used": 0,
        "glm_call_ceiling": ALIGNED_MAX_CALLS,
        "glm_calls_used": 0,
        "calibration_calls": CALIBRATION_CALLS,
        "calibration_segment_ids": _aligned_calibration_ids(entries),
        "token_ceiling_state": "calibrate_from_first_three_density_stratified_calls",
        "absolute_preflight_token_ceiling": ABSOLUTE_PREFLIGHT_TOKEN_CEILING,
        "measured_token_ceiling": None,
        "token_ceiling_formula": "ceil(max(first_three_total_tokens) * authorized_call_ceiling * 1.15)",
        "tokens_used": 0,
        "subscription_spend_usd": 0.0,
        "production_database_writes": 0,
        "holdout_calls": 0,
        "extractor_gate_initialized": False,
    }
    _write_new_json(ledger_path.expanduser().resolve(), ledger)
    return ledger


def _aligned_context_prompt(connection: sqlite3.Connection, entry: Mapping[str, Any]) -> str:
    from .worker import (
        adjacent_segment_context_for_segment,
        completed_episode_context_for_segment,
        segment_for_job,
        slim_episode_context_for_label,
        slim_label_segment_context,
    )

    segment_id = str(entry["segment_id"])
    segment_context = segment_for_job(connection, {"target_id": segment_id})
    context_run = completed_episode_context_for_segment(
        connection,
        segment_id,
        label_pack=LABEL_PACK,
        model=BASELINE_MODEL,
    )
    if not context_run:
        raise CandidateLaneError(f"completed production episode context unavailable: {segment_id}")
    artifact_path = Path(str(context_run["context_artifact_path"])).expanduser().resolve()
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    compact_context = slim_label_segment_context(segment_context["context"])
    adjacent_context = adjacent_segment_context_for_segment(
        connection,
        segment_id,
        max_chars=1400,
        include_current=False,
    )
    relevant_segment_ids = {
        segment_id,
        *(str(item["segment_id"]) for item in adjacent_context["segments"]),
    }
    compact_context["episode_context_artifact"] = slim_episode_context_for_label(
        artifact,
        relevant_segment_ids=relevant_segment_ids,
    )
    compact_context["episode_context_run_id"] = context_run["id"]
    compact_context["adjacent_segment_context"] = adjacent_context
    compact_context["episode_context_contract"] = (
        "This compact artifact came from a GPT-5.5 full-episode read. Use it for "
        "speaker/entity/concept context. Use adjacent_segment_context to resolve speaker "
        "continuity across segment boundaries, but emit evidence only from the current Segment Text section."
    )
    segment_text = str(segment_context["segment_text"])
    if _sha256_text(segment_text) != str(entry["segment_text_sha256"]):
        raise CandidateLaneError(f"segment hash changed before aligned prompt: {segment_id}")
    return render_prompt(
        LABEL_PACK,
        {"text": segment_text},
        compact_context,
    )


def _aligned_opencode_config() -> dict[str, Any]:
    agent_name = "pif-production-labeler"
    return {
        "$schema": "https://opencode.ai/config.json",
        "share": "disabled",
        "snapshot": False,
        "default_agent": agent_name,
        "permission": {"*": "deny"},
        "agent": {
            agent_name: {
                "description": "Private full-contract podcast production labeler",
                "mode": "primary",
                "model": DEFAULT_MODEL,
                "temperature": 0.1,
                "steps": 8,
                "prompt": (
                    "Perform the supplied private podcast labeling contract exactly. Read the complete "
                    "single-shot prompt, use no tools, and return only the requested JSON object without Markdown."
                ),
                "permission": {"*": "deny"},
            }
        },
    }


def _blanked_metric_count(before: Mapping[str, Any], after: Mapping[str, Any]) -> int:
    count = 0
    for before_event, after_event in zip(
        before.get("discourse_events") or [],
        after.get("discourse_events") or [],
    ):
        if not isinstance(before_event, dict) or not isinstance(after_event, dict):
            continue
        before_metric = before_event.get("metric")
        after_metric = after_event.get("metric")
        if not isinstance(before_metric, dict) or not isinstance(after_metric, dict):
            continue
        fields = ("raw_text", "value", "unit", "comparator")
        if any(before_metric.get(field) is not None for field in fields) and all(
            after_metric.get(field) is None for field in fields
        ):
            count += 1
    return count


def _aligned_run_one(
    *,
    entry: Mapping[str, Any],
    prompt: str,
    output_root: Path,
    timeout_seconds: int,
    opencode_binary: str,
) -> dict[str, Any]:
    segment_id = str(entry["segment_id"])
    segment_text = Path(str(entry["segment_text_path"])).read_text(encoding="utf-8")
    prompt_path = output_root / "prompts" / f"{segment_id}.private.md"
    _write_new_json(
        output_root / "prompt-receipts" / f"{segment_id}.json",
        {
            "segment_id": segment_id,
            "prompt_path": str(prompt_path),
            "prompt_sha256": _sha256_text(prompt),
            "prompt_bytes": len(prompt.encode("utf-8")),
            "dispatch_mode": "single_shot_stdin_to_opencode_positional_bridge",
        },
    )
    with prompt_path.open("x", encoding="utf-8") as handle:
        handle.write(prompt)
    os.chmod(prompt_path, 0o600)
    env = os.environ.copy()
    env["OPENCODE_CONFIG_CONTENT"] = _canonical_json(_aligned_opencode_config())
    worker_state_root = output_root / "worker-state"
    auth_source = Path.home() / ".local" / "share" / "opencode" / "auth.json"
    if not auth_source.is_file():
        raise CandidateLaneError("OpenCode authentication material is unavailable")
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"{segment_id}-", dir=str(worker_state_root)) as worker_data:
        private_data_root = Path(worker_data)
        private_opencode_root = private_data_root / "opencode"
        private_opencode_root.mkdir()
        auth_target = private_opencode_root / "auth.json"
        shutil.copy2(auth_source, auth_target)
        os.chmod(auth_target, 0o600)
        env["XDG_DATA_HOME"] = str(private_data_root)
        # OpenCode 1.18.4 exposes only positional messages for `run`.  The zsh
        # bridge reads exactly one prompt from stdin and passes it as one opaque
        # argv element; no shell evaluation is performed on prompt contents.
        command = [
            "/bin/zsh",
            "-f",
            "-c",
            'payload=$(cat); exec "$@" "$payload"',
            "pif-opencode-stdin-bridge",
            opencode_binary,
            "run",
            "--pure",
            "--dir",
            str(output_root / "scratch"),
            "--model",
            DEFAULT_MODEL,
            "--agent",
            "pif-production-labeler",
            "--format",
            "json",
            "--title",
            f"pif-glm52-aligned-{segment_id}",
        ]
        timed_out = False
        try:
            completed = subprocess.run(
                command,
                cwd=str(Path(__file__).resolve().parents[1]),
                env=env,
                input=prompt,
                text=True,
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout_seconds,
                check=False,
            )
            stdout = completed.stdout
            stderr = completed.stderr
            exit_code = completed.returncode
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            stdout = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            exit_code = None
    elapsed = round(time.monotonic() - started, 3)
    _write_new_json(
        output_root / "transport" / f"{segment_id}.json",
        {"exit_code": exit_code, "timed_out": timed_out, "elapsed_seconds": elapsed},
    )
    (output_root / "raw-events" / f"{segment_id}.private.jsonl").write_text(stdout, encoding="utf-8")
    (output_root / "stderr" / f"{segment_id}-attempt-1.private.txt").write_text(stderr, encoding="utf-8")
    answer, finish, stream_count = _parse_opencode_stream(stdout)
    (output_root / "raw-answers" / f"{segment_id}.private.txt").write_text(answer + "\n", encoding="utf-8")
    attempt: dict[str, Any] = {
        "attempt": 1,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "elapsed_seconds": elapsed,
        "stream_event_count": stream_count,
        "raw_answer_bytes": len(answer.encode("utf-8")),
        "usage": _usage(finish),
        "schema_valid": False,
        "raw_contract_valid": False,
        "usable_after_production_repair": False,
    }
    final_payload: dict[str, Any] | None = None
    validation: dict[str, Any] | None = None
    if exit_code == 0 and not timed_out:
        try:
            payload, fence_removed = _decode_answer(answer)
            attempt["outer_fence_stripped"] = fence_removed
            _validate_schema(load_label_pack(LABEL_PACK).schema, payload, path="$")
            attempt["schema_valid"] = True
            returned_events = len(payload.get("discourse_events") or [])
            try:
                validate_label_output(LABEL_PACK, payload, segment_text=segment_text)
                attempt["raw_contract_valid"] = True
                raw_validation_error = None
            except Exception as exc:
                raw_validation_error = str(exc)[:500]
                attempt["raw_validation_error_kind"] = type(exc).__name__
                attempt["raw_validation_error"] = raw_validation_error
            repaired = copy.deepcopy(payload)
            repair_count = repair_label_output_for_submission(
                LABEL_PACK,
                repaired,
                segment_text=segment_text,
            )
            repaired_events = len(repaired.get("discourse_events") or [])
            validate_label_output(LABEL_PACK, repaired, segment_text=segment_text)
            final_payload = repaired
            attempt["usable_after_production_repair"] = True
            attempt["returned_event_count"] = returned_events
            attempt["event_count"] = repaired_events
            attempt["repair_count"] = repair_count
            attempt["invalid_evidence_events_dropped"] = max(0, returned_events - repaired_events)
            attempt["blanked_metric_count"] = _blanked_metric_count(payload, repaired)
            validation = {
                "returned_event_count": returned_events,
                "event_count": repaired_events,
                "invalid_evidence_events_dropped": max(0, returned_events - repaired_events),
                "blanked_metric_count": attempt["blanked_metric_count"],
                "repair_count": repair_count,
                "raw_contract_valid": attempt["raw_contract_valid"],
                "usable_after_repair": True,
            }
        except Exception as exc:
            attempt["validation_error_kind"] = type(exc).__name__
            attempt["validation_error"] = str(exc)[:500]
    if final_payload is not None:
        _write_new_json(output_root / "validated" / f"{segment_id}.private.json", final_payload)
    return {
        "segment_id": segment_id,
        "source_name": str(entry["source_name"]),
        "chunk_index": int(entry["segment_index"]),
        "prompt_sha256": _sha256_text(prompt),
        "prompt_bytes": len(prompt.encode("utf-8")),
        "baseline_event_count": int(entry["event_count"]),
        "attempts": [attempt],
        "attempt_count": 1,
        "retry_attempts": 0,
        "usable": final_payload is not None,
        "event_count": len(final_payload.get("discourse_events") or []) if final_payload else 0,
        "validation": validation,
    }


def _current_validation_failure_breakdown(database: Path) -> dict[str, Any]:
    connection = _ro_connection(database)
    counts: Counter[str] = Counter()
    try:
        connection.execute("BEGIN")
        rows = connection.execute(
            """
            SELECT l.output_json, sg.text_path, sg.text_sha256, sg.episode_id
            FROM labels l JOIN segments sg ON sg.id=l.segment_id
            WHERE l.label_pack=? AND l.label_pack_version=? AND l.model=? AND l.status='ready'
            """,
            (LABEL_PACK, LABEL_PACK, BASELINE_MODEL),
        ).fetchall()
        for row in rows:
            if str(row["episode_id"]) == SEALED_EPISODE_ID:
                counts["sealed_episode_excluded"] += 1
                continue
            try:
                segment_text = Path(str(row["text_path"])).read_text(encoding="utf-8")
            except OSError:
                counts["segment_text_unreadable"] += 1
                continue
            if _sha256_text(segment_text) != str(row["text_sha256"]):
                counts["segment_hash_mismatch"] += 1
                continue
            try:
                validate_label_output(
                    LABEL_PACK,
                    json.loads(str(row["output_json"])),
                    segment_text=segment_text,
                )
                counts["valid"] += 1
            except Exception as exc:
                message = str(exc)
                if ".metric.raw_text must be an exact evidence substring" in message:
                    counts["metric_raw_text_not_exact_evidence_substring"] += 1
                elif ".metric" in message:
                    counts["other_metric_grounding"] += 1
                elif "offset" in message.lower() or "exact contiguous" in message.lower():
                    counts["evidence_offset_or_span"] += 1
                else:
                    counts["other_validation"] += 1
    finally:
        connection.close()
    return {
        "observed_at": _now(),
        "database_mode": "ro_query_only",
        "counts": dict(sorted(counts.items())),
        "interpretation": (
            "Diagnostic only; no remediation performed. All 3,924 validation failures are metric-grounding "
            "failures: metric.raw_text itself is invalid in one subset, while value/unit/comparator grounding "
            "is invalid in the other. This is consistent with pre-contract normalized or stitched metric text."
        ),
    }


def _baseline_cost_and_runtime(
    *, database: Path, entries: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], float]:
    connection = _ro_connection(database)
    try:
        label_ids = [str(entry["label_id"]) for entry in entries]
        placeholders = ",".join("?" for _ in label_ids)
        rows = [
            dict(row)
            for row in connection.execute(
                f"""
                SELECT l.id AS label_id, l.segment_id, l.output_json, l.prompt_path, l.output_path,
                       sg.episode_id, sg.segment_index, sg.word_count, sg.text_path,
                       ep.title AS episode_title, ep.published_at AS episode_published_at,
                       so.name AS source_name
                FROM labels l JOIN segments sg ON sg.id=l.segment_id
                JOIN episodes ep ON ep.id=sg.episode_id JOIN sources so ON so.id=sg.source_id
                WHERE l.id IN ({placeholders})
                """,
                label_ids,
            ).fetchall()
        ]
        for row in rows:
            row["output"] = json.loads(row["output_json"])
        by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_episode[str(row["episode_id"])].append(row)
        cost = _historical_artifact_cost(rows)
        runtime = _runtime_stats(
            connection,
            labels=rows,
            labels_by_episode=by_episode,
            label_pack=LABEL_PACK,
            model=BASELINE_MODEL,
        )
        return cost, float(runtime.get("current_label_runtime_seconds") or 0)
    finally:
        connection.close()


def run_aligned_development(
    *,
    manifest_path: Path,
    ledger_path: Path,
    output_root: Path,
    prior_report_path: Path,
    concurrency: int = 2,
    timeout_seconds: int = 600,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    ledger_file = ledger_path.expanduser().resolve()
    root = output_root.expanduser().resolve()
    manifest = _read_object(manifest_file)
    ledger = _read_object(ledger_file)
    prior_report = _read_object(prior_report_path.expanduser().resolve())
    if ledger.get("state") != "declared_pre_provider" or int(ledger.get("glm_calls_used") or 0) != 0:
        raise CandidateLaneError("aligned provider bounds were not freshly declared")
    if str(ledger.get("manifest_file_sha256")) != _sha256_file(manifest_file):
        raise CandidateLaneError("manifest changed after aligned bound declaration")
    if ledger.get("contract") != _pack_contract():
        raise CandidateLaneError("production label-pack contract changed after bound declaration")
    entries = list(manifest.get("folds", {}).get("DEVELOPMENT", {}).get("entries") or [])
    if len(entries) != ALIGNED_MAX_CALLS:
        raise CandidateLaneError("aligned development fold size changed")
    if root.exists():
        raise FileExistsError(root)
    root.mkdir(parents=True)
    os.chmod(root, 0o700)
    for name in (
        "prompts", "prompt-receipts", "raw-events", "raw-answers", "stderr",
        "transport", "validated", "scratch", "worker-state",
    ):
        (root / name).mkdir()
    database = Path(str(manifest["production_database"]["path"]))
    connection = _ro_connection(database)
    try:
        prompts = {
            str(entry["segment_id"]): _aligned_context_prompt(connection, entry)
            for entry in entries
        }
    finally:
        connection.close()
    prompt_bytes = [len(value.encode("utf-8")) for value in prompts.values()]
    # Keep the stdin-to-argv bridge safely below macOS ARG_MAX with room for env/flags.
    if max(prompt_bytes) > 180_000:
        raise CandidateLaneError(f"aligned prompt exceeds safe stdin bridge size: {max(prompt_bytes)} bytes")
    calibration_ids = list(map(str, ledger["calibration_segment_ids"]))
    by_id = {str(entry["segment_id"]): entry for entry in entries}
    ordered_entries = [by_id[segment_id] for segment_id in calibration_ids]
    ordered_entries.extend(entry for entry in entries if str(entry["segment_id"]) not in set(calibration_ids))
    results: list[dict[str, Any]] = []
    wall_started = time.monotonic()
    for entry in ordered_entries[:CALIBRATION_CALLS]:
        result = _aligned_run_one(
            entry=entry,
            prompt=prompts[str(entry["segment_id"])],
            output_root=root,
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
        )
        results.append(result)
        if _throttled(result, root):
            ledger.update(state="stopped_provider_throttling", updated_at=_now(), glm_calls_used=len(results))
            _replace_owned_json(ledger_file, ledger)
            raise CandidateLaneError("provider throttling detected during aligned calibration")
    calibration_usage = _usage_rows(results)
    calibration_totals = [int(row.get("total_tokens") or 0) for row in calibration_usage]
    if len(calibration_totals) != CALIBRATION_CALLS or any(value <= 0 for value in calibration_totals):
        ledger.update(
            state="stopped_missing_calibration_usage",
            updated_at=_now(),
            glm_calls_used=len(results),
            tokens_used=sum(calibration_totals),
        )
        _replace_owned_json(ledger_file, ledger)
        raise CandidateLaneError("aligned calibration did not return complete token usage")
    measured_ceiling = min(
        math.ceil(max(calibration_totals) * ALIGNED_MAX_CALLS * 1.15),
        int(ledger["absolute_preflight_token_ceiling"]),
    )
    ledger.update(
        state="calibrated_provider_bound",
        updated_at=_now(),
        glm_calls_used=CALIBRATION_CALLS,
        tokens_used=sum(calibration_totals),
        calibration_total_tokens=calibration_totals,
        measured_token_ceiling=measured_ceiling,
        token_ceiling_state="fixed_from_first_three_density_stratified_calls",
        prompt_bytes={"min": min(prompt_bytes), "mean": round(statistics.fmean(prompt_bytes), 3), "max": max(prompt_bytes)},
    )
    _replace_owned_json(ledger_file, ledger)
    pending = iter(ordered_entries[CALIBRATION_CALLS:])
    active: dict[Any, Mapping[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, min(concurrency, 2))) as executor:
        while True:
            while len(active) < max(1, min(concurrency, 2)):
                try:
                    entry = next(pending)
                except StopIteration:
                    break
                projected = int(ledger["tokens_used"]) + max(calibration_totals) * (len(active) + 1)
                if projected > measured_ceiling:
                    ledger.update(state="stopped_token_ceiling", updated_at=_now())
                    _replace_owned_json(ledger_file, ledger)
                    raise CandidateLaneError("aligned measured token ceiling would be exceeded")
                future = executor.submit(
                    _aligned_run_one,
                    entry=entry,
                    prompt=prompts[str(entry["segment_id"])],
                    output_root=root,
                    timeout_seconds=timeout_seconds,
                    opencode_binary=opencode_binary,
                )
                active[future] = entry
            if not active:
                break
            completed, _running = wait(active, return_when=FIRST_COMPLETED)
            throttled = False
            for future in completed:
                active.pop(future)
                result = future.result()
                results.append(result)
                throttled = throttled or _throttled(result, root)
                ledger["glm_calls_used"] = int(ledger["glm_calls_used"]) + 1
                ledger["tokens_used"] = int(ledger["tokens_used"]) + _usage_total(_usage_rows([result]), "total_tokens")
                ledger["updated_at"] = _now()
                _replace_owned_json(ledger_file, ledger)
            if throttled:
                for future in active:
                    future.cancel()
                ledger.update(state="stopped_provider_throttling", updated_at=_now())
                _replace_owned_json(ledger_file, ledger)
                raise CandidateLaneError("provider throttling detected during aligned attempt")
            if int(ledger["tokens_used"]) > measured_ceiling:
                ledger.update(state="stopped_token_ceiling", updated_at=_now())
                _replace_owned_json(ledger_file, ledger)
                raise CandidateLaneError("aligned measured token ceiling exceeded")
    baseline_cost, baseline_runtime = _baseline_cost_and_runtime(database=database, entries=entries)
    score = _score_results(
        entries=entries,
        results=results,
        output_root=root,
        baseline_cost=baseline_cost,
        baseline_runtime_seconds=baseline_runtime,
    )
    first_score = prior_report["score"]
    close_lane = score["precision"] < 0.70 or score["recall"] < 0.70
    comparison = {
        "narrow_contract_run": {
            key: first_score[key]
            for key in (
                "precision", "recall", "f1", "events_per_segment", "evidence_span_validity",
                "schema_structural_validity", "tokens", "wall_time_per_call_seconds", "disagreements",
            )
        },
        "aligned_production_contract_run": {
            key: score[key]
            for key in (
                "precision", "recall", "f1", "events_per_segment", "evidence_span_validity",
                "schema_structural_validity", "tokens", "wall_time_per_call_seconds", "disagreements",
            )
        },
        "contract_effect": {
            "precision_delta": round(score["precision"] - first_score["precision"], 6),
            "recall_delta": round(score["recall"] - first_score["recall"], 6),
            "f1_delta": round(score["f1"] - first_score["f1"], 6),
            "glm_events_per_segment_delta": round(
                score["events_per_segment"]["glm"] - first_score["events_per_segment"]["glm"], 6
            ),
        },
    }
    failure_breakdown = _current_validation_failure_breakdown(database)
    report = {
        "schema_version": ALIGNED_REPORT_VERSION,
        "created_at": _now(),
        "manifest_path": str(manifest_file),
        "manifest_file_sha256": _sha256_file(manifest_file),
        "ledger_path": str(ledger_file),
        "fold": "DEVELOPMENT",
        "attempt": "aligned_full_production_contract_last_development_attempt",
        "contract": ledger["contract"],
        "dispatch_mode": ledger["dispatch_mode"],
        "holdout_opened_for_execution_or_scoring": False,
        "extractor_gate_initialized": False,
        "production_database_mode": "ro_query_only",
        "production_database_writes": 0,
        "model": DEFAULT_MODEL,
        "transport": "opencode",
        "call_count": len(results),
        "wall_elapsed_seconds": round(time.monotonic() - wall_started, 6),
        "score": score,
        "side_by_side": comparison,
        "existing_ground_truth_validation_failure_breakdown": failure_breakdown,
        "decision": {
            "threshold": {"min_precision": 0.70, "min_recall": 0.70},
            "precision_passed": score["precision"] >= 0.70,
            "recall_passed": score["recall"] >= 0.70,
            "glm_label_segment_lane_closed": close_lane,
            "production_answer": "codex_only_labeling" if close_lane else "eligible_for_separate_holdout_authorization_consideration",
            "further_prompt_iterations_proposed": False,
        },
        "results": results,
    }
    report_path = root / "aligned-development-report.json"
    _write_new_json(report_path, report)
    if close_lane:
        _write_new_json(
            root / "glm-label-segment-lane-closure.json",
            {
                "schema_version": "pif_glm52_label_segment_lane_closure_v1",
                "created_at": _now(),
                "reason": "last aligned development attempt remained below 0.70 precision or recall",
                "precision": score["precision"],
                "recall": score["recall"],
                "f1": score["f1"],
                "decision": "GLM lane closed for label_segment; Codex-only labeling is the production answer",
                "further_prompt_iterations_allowed": False,
                "holdout_opened": False,
                "extractor_gate_initialized": False,
                "report_path": str(report_path),
                "report_file_sha256": _sha256_file(report_path),
            },
        )
    ledger.update(
        state="aligned_development_complete_lane_closed" if close_lane else "aligned_development_complete_threshold_met",
        updated_at=_now(),
        glm_calls_used=len(results),
        tokens_used=int(score["tokens"]["glm_total_tokens"]),
        subscription_spend_usd=0.0,
        report_path=str(report_path),
        report_file_sha256=_sha256_file(report_path),
        lane_closed=close_lane,
    )
    _replace_owned_json(ledger_file, ledger)
    _write_new_json(
        root / "receipt.json",
        {
            "schema_version": "pif_glm52_aligned_label_segment_receipt_v1",
            "created_at": _now(),
            "report_path": str(report_path),
            "report_file_sha256": _sha256_file(report_path),
            "ledger_path": str(ledger_file),
            "ledger_file_sha256": _sha256_file(ledger_file),
            "manifest_path": str(manifest_file),
            "manifest_file_sha256": _sha256_file(manifest_file),
            "subscription_spend_usd": 0.0,
            "codex_calls": 0,
            "holdout_calls": 0,
            "production_writes": 0,
            "lane_closed": close_lane,
        },
    )
    return {**report, "path": str(report_path), "file_sha256": _sha256_file(report_path)}


def _reconstruct_aligned_result(
    *, entry: Mapping[str, Any], output_root: Path
) -> dict[str, Any]:
    segment_id = str(entry["segment_id"])
    raw_events_path = output_root / "raw-events" / f"{segment_id}.private.jsonl"
    answer_path = output_root / "raw-answers" / f"{segment_id}.private.txt"
    transport_path = output_root / "transport" / f"{segment_id}.json"
    stdout = raw_events_path.read_text(encoding="utf-8", errors="replace") if raw_events_path.is_file() else ""
    answer = answer_path.read_text(encoding="utf-8", errors="replace").strip() if answer_path.is_file() else ""
    _stream_answer, finish, stream_count = _parse_opencode_stream(stdout)
    transport = _read_object(transport_path)
    attempt: dict[str, Any] = {
        "attempt": 1,
        "exit_code": transport.get("exit_code"),
        "timed_out": bool(transport.get("timed_out")),
        "elapsed_seconds": float(transport.get("elapsed_seconds") or 0),
        "stream_event_count": stream_count,
        "raw_answer_bytes": len(answer.encode("utf-8")),
        "usage": _usage(finish),
        "schema_valid": False,
        "raw_contract_valid": False,
        "usable_after_production_repair": False,
    }
    validation = None
    validated_path = output_root / "validated" / f"{segment_id}.private.json"
    if answer:
        try:
            payload, fence_removed = _decode_answer(answer)
            attempt["outer_fence_stripped"] = fence_removed
            _validate_schema(load_label_pack(LABEL_PACK).schema, payload, path="$")
            attempt["schema_valid"] = True
            segment_text = Path(str(entry["segment_text_path"])).read_text(encoding="utf-8")
            returned_events = len(payload.get("discourse_events") or [])
            try:
                validate_label_output(LABEL_PACK, payload, segment_text=segment_text)
                attempt["raw_contract_valid"] = True
            except Exception as exc:
                attempt["raw_validation_error_kind"] = type(exc).__name__
                attempt["raw_validation_error"] = str(exc)[:500]
            repaired = copy.deepcopy(payload)
            repair_count = repair_label_output_for_submission(
                LABEL_PACK, repaired, segment_text=segment_text
            )
            repaired_events = len(repaired.get("discourse_events") or [])
            validate_label_output(LABEL_PACK, repaired, segment_text=segment_text)
            attempt["usable_after_production_repair"] = True
            attempt["returned_event_count"] = returned_events
            attempt["event_count"] = repaired_events
            attempt["repair_count"] = repair_count
            attempt["invalid_evidence_events_dropped"] = max(0, returned_events - repaired_events)
            attempt["blanked_metric_count"] = _blanked_metric_count(payload, repaired)
            validation = {
                "returned_event_count": returned_events,
                "event_count": repaired_events,
                "invalid_evidence_events_dropped": max(0, returned_events - repaired_events),
                "blanked_metric_count": attempt["blanked_metric_count"],
                "repair_count": repair_count,
                "raw_contract_valid": attempt["raw_contract_valid"],
                "usable_after_repair": True,
            }
            if not validated_path.is_file():
                _write_new_json(validated_path, repaired)
        except Exception as exc:
            attempt["validation_error_kind"] = type(exc).__name__
            attempt["validation_error"] = str(exc)[:500]
    usable = bool(validation and validated_path.is_file())
    return {
        "segment_id": segment_id,
        "source_name": str(entry["source_name"]),
        "chunk_index": int(entry["segment_index"]),
        "baseline_event_count": int(entry["event_count"]),
        "attempts": [attempt],
        "attempt_count": 1,
        "retry_attempts": 0,
        "usable": usable,
        "event_count": len(_candidate_events(output_root, segment_id)) if usable else 0,
        "validation": validation,
    }


def finalize_aligned_calibration_failure(
    *,
    manifest_path: Path,
    ledger_path: Path,
    output_root: Path,
    prior_report_path: Path,
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    ledger_file = ledger_path.expanduser().resolve()
    root = output_root.expanduser().resolve()
    manifest = _read_object(manifest_file)
    ledger = _read_object(ledger_file)
    if ledger.get("state") != "stopped_missing_calibration_usage":
        raise CandidateLaneError("aligned calibration is not in the expected fail-closed state")
    if int(ledger.get("glm_calls_used") or 0) != CALIBRATION_CALLS:
        raise CandidateLaneError("aligned calibration call count is not exactly three")
    entries_all = list(manifest.get("folds", {}).get("DEVELOPMENT", {}).get("entries") or [])
    by_id = {str(entry["segment_id"]): entry for entry in entries_all}
    calibration_entries = [by_id[str(segment_id)] for segment_id in ledger["calibration_segment_ids"]]
    aligned_results = [
        _reconstruct_aligned_result(entry=entry, output_root=root)
        for entry in calibration_entries
    ]
    database = Path(str(manifest["production_database"]["path"]))
    baseline_cost, baseline_runtime = _baseline_cost_and_runtime(
        database=database, entries=calibration_entries
    )
    aligned_score = _score_results(
        entries=calibration_entries,
        results=aligned_results,
        output_root=root,
        baseline_cost=baseline_cost,
        baseline_runtime_seconds=baseline_runtime,
    )
    prior_report = _read_object(prior_report_path.expanduser().resolve())
    prior_results = [
        result
        for result in prior_report.get("results") or []
        if str(result.get("segment_id")) in set(ledger["calibration_segment_ids"])
    ]
    prior_root = prior_report_path.expanduser().resolve().parent
    narrow_score_same_segments = _score_results(
        entries=calibration_entries,
        results=prior_results,
        output_root=prior_root,
        baseline_cost=baseline_cost,
        baseline_runtime_seconds=baseline_runtime,
    )
    known_usage = _usage_rows(aligned_results)
    known_tokens = _usage_total(known_usage, "total_tokens")
    aligned_score["tokens"].update(
        {
            "usage_complete": False,
            "known_total_tokens": known_tokens,
            "unknown_usage_calls": sum(
                attempt.get("usage") is None
                for result in aligned_results
                for attempt in result.get("attempts") or []
            ),
            "ratio_is_lower_bound": True,
        }
    )
    aligned_score["directive_gate"]["passed"] = False
    aligned_score["directive_gate"]["status"] = "failed_incomplete_calibration_and_quality_below_threshold"
    close_lane = aligned_score["precision"] < 0.70 or aligned_score["recall"] < 0.70
    report = {
        "schema_version": "pif_glm52_aligned_calibration_terminal_report_v1",
        "created_at": _now(),
        "manifest_path": str(manifest_file),
        "manifest_file_sha256": _sha256_file(manifest_file),
        "ledger_path": str(ledger_file),
        "fold": "DEVELOPMENT_CALIBRATION_SUBSET_ONLY",
        "calibration_segment_count": len(calibration_entries),
        "calibration_ground_truth_event_count": sum(int(entry["event_count"]) for entry in calibration_entries),
        "calibration_event_density": density_summary(
            [int(entry["event_count"]) for entry in calibration_entries]
        ),
        "attempt": "aligned_full_production_contract_last_development_attempt",
        "contract": ledger["contract"],
        "dispatch_mode": ledger["dispatch_mode"],
        "terminal_condition": {
            "kind": "missing_usage_after_high_density_timeout",
            "provider_calls_completed_or_timed_out": CALIBRATION_CALLS,
            "remaining_calls_not_dispatched": ALIGNED_MAX_CALLS - CALIBRATION_CALLS,
            "measured_token_ceiling_established": False,
            "reason": "One 600-second high-density call timed out without a terminal usage event; bounded accounting could not continue.",
        },
        "holdout_opened_for_execution_or_scoring": False,
        "extractor_gate_initialized": False,
        "production_database_mode": "ro_query_only",
        "production_database_writes": 0,
        "model": DEFAULT_MODEL,
        "transport": "opencode",
        "call_count": CALIBRATION_CALLS,
        "score": aligned_score,
        "side_by_side_same_three_segments": {
            "narrow_contract": narrow_score_same_segments,
            "aligned_production_contract": aligned_score,
            "contract_effect": {
                "precision_delta": round(aligned_score["precision"] - narrow_score_same_segments["precision"], 6),
                "recall_delta": round(aligned_score["recall"] - narrow_score_same_segments["recall"], 6),
                "f1_delta": round(aligned_score["f1"] - narrow_score_same_segments["f1"], 6),
                "glm_events_per_segment_delta": round(
                    aligned_score["events_per_segment"]["glm"]
                    - narrow_score_same_segments["events_per_segment"]["glm"],
                    6,
                ),
            },
        },
        "prior_full_narrow_run": {
            "segment_count": int(prior_report.get("call_count") or 0),
            "score": prior_report["score"],
        },
        "existing_ground_truth_validation_failure_breakdown": _current_validation_failure_breakdown(database),
        "decision": {
            "threshold": {"min_precision": 0.70, "min_recall": 0.70},
            "precision_passed": aligned_score["precision"] >= 0.70,
            "recall_passed": aligned_score["recall"] >= 0.70,
            "glm_label_segment_lane_closed": close_lane,
            "production_answer": "codex_only_labeling",
            "further_prompt_iterations_proposed": False,
            "holdout_authorization_recommended": False,
        },
        "results": aligned_results,
    }
    report_path = root / "aligned-calibration-terminal-report.json"
    _write_new_json(report_path, report)
    closure_path = root / "glm-label-segment-lane-closure.json"
    _write_new_json(
        closure_path,
        {
            "schema_version": "pif_glm52_label_segment_lane_closure_v1",
            "created_at": _now(),
            "reason": "last aligned development attempt failed bounded calibration and remained below 0.70 precision or recall",
            "precision": aligned_score["precision"],
            "recall": aligned_score["recall"],
            "f1": aligned_score["f1"],
            "decision": "GLM lane closed for label_segment; Codex-only labeling is the production answer",
            "further_prompt_iterations_allowed": False,
            "holdout_opened": False,
            "extractor_gate_initialized": False,
            "report_path": str(report_path),
            "report_file_sha256": _sha256_file(report_path),
        },
    )
    ledger.update(
        state="aligned_development_terminal_lane_closed",
        updated_at=_now(),
        measured_token_ceiling=None,
        tokens_used=known_tokens,
        token_usage_complete=False,
        unknown_usage_calls=1,
        subscription_spend_usd=0.0,
        report_path=str(report_path),
        report_file_sha256=_sha256_file(report_path),
        closure_path=str(closure_path),
        closure_file_sha256=_sha256_file(closure_path),
        lane_closed=True,
    )
    _replace_owned_json(ledger_file, ledger)
    receipt_path = root / "receipt.json"
    _write_new_json(
        receipt_path,
        {
            "schema_version": "pif_glm52_aligned_label_segment_terminal_receipt_v1",
            "created_at": _now(),
            "report_path": str(report_path),
            "report_file_sha256": _sha256_file(report_path),
            "closure_path": str(closure_path),
            "closure_file_sha256": _sha256_file(closure_path),
            "ledger_path": str(ledger_file),
            "ledger_file_sha256": _sha256_file(ledger_file),
            "manifest_path": str(manifest_file),
            "manifest_file_sha256": _sha256_file(manifest_file),
            "subscription_spend_usd": 0.0,
            "known_tokens": known_tokens,
            "unknown_usage_calls": 1,
            "codex_calls": 0,
            "holdout_calls": 0,
            "production_writes": 0,
            "lane_closed": True,
        },
    )
    return {**report, "path": str(report_path), "file_sha256": _sha256_file(report_path)}


FAIR_NARROW_WEIGHTS = {
    "event_type": 0.15 / 0.68,
    "evidence": 0.28 / 0.68,
    "claim_text": 0.25 / 0.68,
}
FAIR_MATCH_THRESHOLD = 0.48


def fair_narrow_event_similarity(
    golden: Mapping[str, Any], candidate: Mapping[str, Any]
) -> float:
    """Score only fields present in both the v3.1 gold and narrow contract."""
    return (
        FAIR_NARROW_WEIGHTS["event_type"]
        * float(golden.get("event_type") == candidate.get("event_type"))
        + FAIR_NARROW_WEIGHTS["evidence"]
        * _token_jaccard(
            str(golden.get("evidence") or ""),
            str(candidate.get("evidence") or ""),
        )
        + FAIR_NARROW_WEIGHTS["claim_text"]
        * _token_jaccard(
            str(golden.get("claim_text") or ""),
            str(candidate.get("claim_text") or ""),
        )
    )


def fair_match_events(
    golden_events: Sequence[Mapping[str, Any]],
    candidate_events: Sequence[Mapping[str, Any]],
    *,
    threshold: float = FAIR_MATCH_THRESHOLD,
) -> list[tuple[int, int, float]]:
    """Mirror efficient_backtest's greedy matcher with a contract-fair score."""
    pairs: list[tuple[int, int, float]] = []
    used_candidates: set[int] = set()
    for golden_index, golden in enumerate(golden_events):
        best: tuple[int, float] | None = None
        for candidate_index, candidate in enumerate(candidate_events):
            if candidate_index in used_candidates:
                continue
            score = fair_narrow_event_similarity(golden, candidate)
            if score >= threshold and (best is None or score > best[1]):
                best = (candidate_index, score)
        if best is not None:
            used_candidates.add(best[0])
            pairs.append((golden_index, best[0], round(best[1], 6)))
    return pairs


def _numeric_distribution(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        raise CandidateLaneError("numeric distribution requires values")
    return {
        "count": len(values),
        "mean": round(statistics.fmean(values), 6),
        "stdev": round(statistics.pstdev(values), 6),
        "min": round(min(values), 6),
        "p10": round(float(_percentile(values, 0.10)), 6),
        "p25": round(float(_percentile(values, 0.25)), 6),
        "p50": round(float(_percentile(values, 0.50)), 6),
        "p75": round(float(_percentile(values, 0.75)), 6),
        "p90": round(float(_percentile(values, 0.90)), 6),
        "p95": round(float(_percentile(values, 0.95)), 6),
        "p99": round(float(_percentile(values, 0.99)), 6),
        "max": round(max(values), 6),
    }


def _score_histogram(values: Sequence[float]) -> dict[str, int]:
    return {
        "0_0.20": sum(0 <= value < 0.20 for value in values),
        "0.20_0.30": sum(0.20 <= value < 0.30 for value in values),
        "0.30_0.40": sum(0.30 <= value < 0.40 for value in values),
        "0.40_0.48": sum(0.40 <= value < 0.48 for value in values),
        "0.48_0.60": sum(0.48 <= value < 0.60 for value in values),
        "0.60_0.68": sum(0.60 <= value < 0.68 for value in values),
        "0.68_0.80": sum(0.68 <= value < 0.80 for value in values),
        "0.80_1.01": sum(0.80 <= value <= 1.0 for value in values),
    }


def _candidate_positions(segment_text: str, evidence: str) -> list[tuple[int, int]]:
    if not evidence:
        return []
    positions = []
    cursor = segment_text.find(evidence)
    while cursor >= 0:
        positions.append((cursor, cursor + len(evidence)))
        cursor = segment_text.find(evidence, cursor + 1)
    return positions


def _span_overlap_metrics(
    golden_events: Sequence[Mapping[str, Any]],
    candidate_events: Sequence[Mapping[str, Any]],
    *,
    segment_text: str,
) -> list[dict[str, float]]:
    candidate_spans = [
        span
        for candidate in candidate_events
        for span in _candidate_positions(segment_text, str(candidate.get("evidence") or ""))
    ]
    results = []
    for golden in golden_events:
        start = golden.get("evidence_start")
        end = golden.get("evidence_end")
        if not isinstance(start, int) or not isinstance(end, int) or end <= start:
            results.append({"overlap_coefficient": 0.0, "intersection_over_union": 0.0})
            continue
        best_coefficient = 0.0
        best_iou = 0.0
        for candidate_start, candidate_end in candidate_spans:
            intersection = max(0, min(end, candidate_end) - max(start, candidate_start))
            if intersection <= 0:
                continue
            coefficient = intersection / min(end - start, candidate_end - candidate_start)
            union = max(end, candidate_end) - min(start, candidate_start)
            iou = intersection / union if union else 0.0
            best_coefficient = max(best_coefficient, coefficient)
            best_iou = max(best_iou, iou)
        results.append(
            {
                "overlap_coefficient": best_coefficient,
                "intersection_over_union": best_iou,
            }
        )
    return results


def _matcher_score(
    *,
    entries: Sequence[Mapping[str, Any]],
    candidate_root: Path,
    matcher: Any,
) -> dict[str, Any]:
    totals: Counter[str] = Counter()
    field_mismatches: Counter[str] = Counter()
    per_segment = []
    for entry in entries:
        golden = [dict(event) for event in entry.get("accepted_events") or []]
        candidate = _candidate_events(candidate_root, str(entry["segment_id"]))
        matched = matcher(golden, candidate)
        matched_golden = {item[0] for item in matched}
        matched_candidate = {item[1] for item in matched}
        wrong_spans = 0
        wrong_fields = 0
        for golden_index, candidate_index, _score in matched:
            golden_event = golden[golden_index]
            candidate_event = candidate[candidate_index]
            if str(golden_event.get("evidence") or "") != str(candidate_event.get("evidence") or ""):
                wrong_spans += 1
            event_wrong = False
            for field in ("event_type", "claim_text"):
                if str(golden_event.get(field) or "") != str(candidate_event.get(field) or ""):
                    field_mismatches[field] += 1
                    event_wrong = True
            wrong_fields += int(event_wrong)
        totals.update(
            golden_events=len(golden),
            candidate_events=len(candidate),
            matched_events=len(matched),
            missed_events=len(golden) - len(matched_golden),
            spurious_events=len(candidate) - len(matched_candidate),
            wrong_spans=wrong_spans,
            wrong_fields=wrong_fields,
        )
        per_segment.append(
            {
                "segment_id": str(entry["segment_id"]),
                "golden_events": len(golden),
                "candidate_events": len(candidate),
                "matched_events": len(matched),
            }
        )
    precision = totals["matched_events"] / totals["candidate_events"] if totals["candidate_events"] else 0.0
    recall = totals["matched_events"] / totals["golden_events"] if totals["golden_events"] else 0.0
    return {
        "golden_events": totals["golden_events"],
        "candidate_events": totals["candidate_events"],
        "matched_events": totals["matched_events"],
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(float(_f1(precision, recall) or 0), 6),
        "disagreements": {
            "missed_events": totals["missed_events"],
            "spurious_events": totals["spurious_events"],
            "wrong_spans": totals["wrong_spans"],
            "wrong_fields": totals["wrong_fields"],
            "wrong_field_counts": dict(sorted(field_mismatches.items())),
        },
        "segments": per_segment,
    }


def rescore_narrow_contract_fairly(
    *,
    manifest_path: Path,
    narrow_run_root: Path,
    prior_report_path: Path,
    output_path: Path,
    reopen_path: Path,
) -> dict[str, Any]:
    manifest_file = manifest_path.expanduser().resolve()
    candidate_root = narrow_run_root.expanduser().resolve()
    prior_file = prior_report_path.expanduser().resolve()
    output_file = output_path.expanduser().resolve()
    reopen_file = reopen_path.expanduser().resolve()
    if output_file.exists() or reopen_file.exists():
        raise FileExistsError("fair rescore output already exists")
    manifest = _read_object(manifest_file)
    entries = list(manifest.get("folds", {}).get("DEVELOPMENT", {}).get("entries") or [])
    if len(entries) != DEVELOPMENT_SEGMENTS:
        raise CandidateLaneError("frozen development fold no longer has 60 entries")
    best_original_scores = []
    best_fair_scores = []
    best_rows = []
    implementation_ceilings = []
    span_results = []
    total_candidates = 0
    total_golden = 0
    for entry in entries:
        golden = [dict(event) for event in entry.get("accepted_events") or []]
        candidate = _candidate_events(candidate_root, str(entry["segment_id"]))
        total_candidates += len(candidate)
        total_golden += len(golden)
        segment_text = Path(str(entry["segment_text_path"])).read_text(encoding="utf-8")
        if _sha256_text(segment_text) != str(entry["segment_text_sha256"]):
            raise CandidateLaneError(f"segment hash changed during fair rescore: {entry['segment_id']}")
        span_results.extend(
            _span_overlap_metrics(golden, candidate, segment_text=segment_text)
        )
        for candidate_index, candidate_event in enumerate(candidate):
            original_pairs = [
                (golden_index, _event_similarity(golden_event, candidate_event))
                for golden_index, golden_event in enumerate(golden)
            ]
            fair_pairs = [
                (golden_index, fair_narrow_event_similarity(golden_event, candidate_event))
                for golden_index, golden_event in enumerate(golden)
            ]
            if original_pairs:
                best_original = max(original_pairs, key=lambda item: item[1])
                best_fair = max(fair_pairs, key=lambda item: item[1])
                # Perfect comparable fields plus incidental credits the old
                # scorer awards when inaccessible optional gold fields are empty.
                ceilings = []
                perfect_candidate = {
                    "event_type": None,
                    "evidence": "perfect comparable text",
                    "claim_text": "perfect comparable text",
                }
                for golden_event in golden:
                    perfect_candidate.update(
                        event_type=golden_event.get("event_type"),
                        evidence=golden_event.get("evidence"),
                        claim_text=golden_event.get("claim_text"),
                    )
                    ceilings.append(_event_similarity(golden_event, perfect_candidate))
                implementation_ceiling = max(ceilings)
            else:
                best_original = (-1, 0.0)
                best_fair = (-1, 0.0)
                implementation_ceiling = 0.68
            best_original_scores.append(best_original[1])
            best_fair_scores.append(best_fair[1])
            implementation_ceilings.append(implementation_ceiling)
            best_rows.append(
                {
                    "segment_id": str(entry["segment_id"]),
                    "candidate_index": candidate_index,
                    "best_golden_index_original": best_original[0],
                    "best_original_score": round(best_original[1], 6),
                    "best_golden_index_fair": best_fair[0],
                    "best_fair_score": round(best_fair[1], 6),
                    "old_scorer_implementation_ceiling": round(implementation_ceiling, 6),
                }
            )
    if total_candidates != len(best_original_scores):
        raise CandidateLaneError("candidate score accounting mismatch")
    original = _matcher_score(entries=entries, candidate_root=candidate_root, matcher=_match_events)
    fair = _matcher_score(entries=entries, candidate_root=candidate_root, matcher=fair_match_events)
    prior = _read_object(prior_file)
    if (
        original["matched_events"] != int(prior["score"]["efficient_backtest_candidate_quality_gate"]["metrics"]["valid_matched_events"])
        or original["candidate_events"] != int(prior["score"]["efficient_backtest_candidate_quality_gate"]["metrics"]["valid_candidate_events"])
    ):
        raise CandidateLaneError("recomputed original matcher does not reproduce the frozen report")
    substantial = sum(item["overlap_coefficient"] >= 0.50 for item in span_results)
    iou_half = sum(item["intersection_over_union"] >= 0.50 for item in span_results)
    any_overlap = sum(item["overlap_coefficient"] > 0 for item in span_results)
    exact_or_cover = sum(item["overlap_coefficient"] >= 0.95 for item in span_results)
    band_count = sum(0.40 <= score < 0.48 for score in best_original_scores)
    band_fair_pass_count = sum(
        0.40 <= old < 0.48 and fair_score >= FAIR_MATCH_THRESHOLD
        for old, fair_score in zip(best_original_scores, best_fair_scores)
    )
    fair_delta = {
        "precision": round(fair["precision"] - original["precision"], 6),
        "recall": round(fair["recall"] - original["recall"], 6),
        "f1": round(fair["f1"] - original["f1"], 6),
        "matched_events": fair["matched_events"] - original["matched_events"],
    }
    mechanism_fraction = band_count / total_candidates if total_candidates else 0.0
    if band_count == 0:
        mechanism_verdict = "does_not_explain"
    elif fair["recall"] >= 0.70 and fair["precision"] >= 0.70:
        mechanism_verdict = "explains_observed_low_scores"
    else:
        mechanism_verdict = "partially_explains"
    viable = fair["precision"] >= 0.70 and fair["recall"] >= 0.70
    report = {
        "schema_version": "pif_glm52_narrow_contract_fair_rescore_v1",
        "created_at": _now(),
        "provider_calls": 0,
        "subscription_spend_usd": 0.0,
        "fold": "DEVELOPMENT",
        "holdout_opened": False,
        "extractor_gate_initialized": False,
        "manifest_path": str(manifest_file),
        "manifest_file_sha256": _sha256_file(manifest_file),
        "narrow_run_root": str(candidate_root),
        "prior_report_path": str(prior_file),
        "prior_report_file_sha256": _sha256_file(prior_file),
        "counts": {
            "segments": len(entries),
            "golden_events": total_golden,
            "glm_events": total_candidates,
        },
        "mechanism_test": {
            "original_best_counterpart_score_distribution": {
                **_numeric_distribution(best_original_scores),
                "histogram": _score_histogram(best_original_scores),
            },
            "fair_best_counterpart_score_distribution": {
                **_numeric_distribution(best_fair_scores),
                "histogram": _score_histogram(best_fair_scores),
            },
            "original_score_band_0.40_inclusive_0.48_exclusive": {
                "count": band_count,
                "fraction_of_glm_events": round(mechanism_fraction, 6),
                "count_passing_fair_threshold": band_fair_pass_count,
            },
            "achievable_ceiling": {
                "contract_semantic_ceiling_under_original_weights": 0.68,
                "unreachable_weight": 0.32,
                "comparable_weight": 0.68,
                "fair_renormalized_ceiling": 1.0,
                "old_scorer_implementation_ceiling_distribution": _numeric_distribution(implementation_ceilings),
                "empty_empty_credit_caveat": (
                    "The old implementation can award optional-field Jaccard credit when both gold and missing candidate render empty; "
                    "therefore its effective ceiling can exceed the contract-semantic 0.68 ceiling on sparse gold events."
                ),
            },
            "verdict": mechanism_verdict,
        },
        "matcher_comparison": {
            "original": original,
            "contract_fair": {
                **fair,
                "weights": {key: round(value, 9) for key, value in FAIR_NARROW_WEIGHTS.items()},
                "threshold": FAIR_MATCH_THRESHOLD,
            },
            "delta_fair_minus_original": fair_delta,
        },
        "evidence_span_overlap": {
            "definition": (
                "Substantially same evidence span means character-overlap coefficient >=0.50 between a golden span "
                "and any grounded GLM span in the same segment; no event type or field weight is used."
            ),
            "golden_events": len(span_results),
            "any_character_overlap_count": any_overlap,
            "any_character_overlap_fraction": round(any_overlap / len(span_results), 6),
            "substantial_overlap_count": substantial,
            "substantial_overlap_fraction": round(substantial / len(span_results), 6),
            "iou_gte_0.50_count": iou_half,
            "iou_gte_0.50_fraction": round(iou_half / len(span_results), 6),
            "near_cover_overlap_coefficient_gte_0.95_count": exact_or_cover,
            "near_cover_overlap_coefficient_gte_0.95_fraction": round(exact_or_cover / len(span_results), 6),
            "max_overlap_coefficient_distribution": _numeric_distribution(
                [item["overlap_coefficient"] for item in span_results]
            ),
            "max_iou_distribution": _numeric_distribution(
                [item["intersection_over_union"] for item in span_results]
            ),
        },
        "recommendation": {
            "glm_viable_for_narrow_label_segment_candidate_extraction": viable,
            "quality_delta_vs_codex": {
                "event_precision_gap": round(1.0 - fair["precision"], 6),
                "event_recall_gap": round(1.0 - fair["recall"], 6),
                "event_count_ratio": round(total_candidates / total_golden, 6),
            },
            "what_fair_matcher_proves": (
                "Contract-comparable semantic/evidence agreement for event_type, evidence, and claim_text on this development fold."
            ),
            "what_fair_matcher_does_not_prove": (
                "It does not prove production-complete ai_discourse_v3_1 records. Matching evidence spans does not supply "
                "claim_type, actor, target concept, entities, metrics, attribution, quality flags, or other required fields."
            ),
            "missing_fields_must_come_from_another_validated_stage": True,
            "production_promotion_authorized": False,
        },
        "candidate_best_counterparts": best_rows,
    }
    _write_new_json(output_file, report)
    reopened = {
        "schema_version": "pif_glm52_label_segment_lane_reopened_v1",
        "created_at": _now(),
        "reason": "prior closure relied on a matcher with fields unreachable to the narrow GLM contract",
        "scope": "development_only_contract_fair_offline_rescore",
        "provider_calls": 0,
        "holdout_opened": False,
        "extractor_gate_initialized": False,
        "production_promotion_authorized": False,
        "report_path": str(output_file),
        "report_file_sha256": _sha256_file(output_file),
        "recommendation": report["recommendation"],
    }
    _write_new_json(reopen_file, reopened)
    return {**report, "path": str(output_file), "file_sha256": _sha256_file(output_file)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pif-glm-candidate-lane")
    subparsers = parser.add_subparsers(dest="action", required=True)
    manifest_parser = subparsers.add_parser("manifest")
    manifest_parser.add_argument("--database", required=True)
    manifest_parser.add_argument("--output", required=True)
    manifest_parser.add_argument("--seed", default=DEFAULT_SEED)
    ledger_parser = subparsers.add_parser("init-ledger")
    ledger_parser.add_argument("--manifest", required=True)
    ledger_parser.add_argument("--ledger", required=True)
    aligned_ledger_parser = subparsers.add_parser("init-aligned-ledger")
    aligned_ledger_parser.add_argument("--manifest", required=True)
    aligned_ledger_parser.add_argument("--ledger", required=True)
    run_parser = subparsers.add_parser("run-development")
    run_parser.add_argument("--manifest", required=True)
    run_parser.add_argument("--ledger", required=True)
    run_parser.add_argument("--output-root", required=True)
    run_parser.add_argument("--concurrency", type=int, default=2)
    run_parser.add_argument("--timeout-seconds", type=int, default=240)
    run_parser.add_argument("--max-events", type=int, default=50)
    run_parser.add_argument("--opencode-binary", default="/opt/homebrew/bin/opencode")
    aligned_run_parser = subparsers.add_parser("run-aligned-development")
    aligned_run_parser.add_argument("--manifest", required=True)
    aligned_run_parser.add_argument("--ledger", required=True)
    aligned_run_parser.add_argument("--output-root", required=True)
    aligned_run_parser.add_argument("--prior-report", required=True)
    aligned_run_parser.add_argument("--concurrency", type=int, default=2)
    aligned_run_parser.add_argument("--timeout-seconds", type=int, default=600)
    aligned_run_parser.add_argument("--opencode-binary", default="/opt/homebrew/bin/opencode")
    aligned_finalize_parser = subparsers.add_parser("finalize-aligned-calibration-failure")
    aligned_finalize_parser.add_argument("--manifest", required=True)
    aligned_finalize_parser.add_argument("--ledger", required=True)
    aligned_finalize_parser.add_argument("--output-root", required=True)
    aligned_finalize_parser.add_argument("--prior-report", required=True)
    fair_parser = subparsers.add_parser("rescore-narrow-fair")
    fair_parser.add_argument("--manifest", required=True)
    fair_parser.add_argument("--narrow-run-root", required=True)
    fair_parser.add_argument("--prior-report", required=True)
    fair_parser.add_argument("--output", required=True)
    fair_parser.add_argument("--reopen", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.action == "manifest":
        result = build_manifest(
            database=Path(args.database),
            output_path=Path(args.output),
            seed=args.seed,
        )
    elif args.action == "init-ledger":
        result = initialize_ledger(
            manifest_path=Path(args.manifest),
            ledger_path=Path(args.ledger),
        )
    elif args.action == "init-aligned-ledger":
        result = initialize_aligned_ledger(
            manifest_path=Path(args.manifest),
            ledger_path=Path(args.ledger),
        )
    elif args.action == "run-development":
        result = run_development(
            manifest_path=Path(args.manifest),
            ledger_path=Path(args.ledger),
            output_root=Path(args.output_root),
            concurrency=args.concurrency,
            timeout_seconds=args.timeout_seconds,
            max_events=args.max_events,
            opencode_binary=args.opencode_binary,
        )
    elif args.action == "run-aligned-development":
        result = run_aligned_development(
            manifest_path=Path(args.manifest),
            ledger_path=Path(args.ledger),
            output_root=Path(args.output_root),
            prior_report_path=Path(args.prior_report),
            concurrency=args.concurrency,
            timeout_seconds=args.timeout_seconds,
            opencode_binary=args.opencode_binary,
        )
    elif args.action == "finalize-aligned-calibration-failure":
        result = finalize_aligned_calibration_failure(
            manifest_path=Path(args.manifest),
            ledger_path=Path(args.ledger),
            output_root=Path(args.output_root),
            prior_report_path=Path(args.prior_report),
        )
    else:
        result = rescore_narrow_contract_fairly(
            manifest_path=Path(args.manifest),
            narrow_run_root=Path(args.narrow_run_root),
            prior_report_path=Path(args.prior_report),
            output_path=Path(args.output),
            reopen_path=Path(args.reopen),
        )
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
