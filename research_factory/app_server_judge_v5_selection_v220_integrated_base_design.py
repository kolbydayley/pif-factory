from __future__ import annotations

"""Freeze a fresh-text integrated extraction and completeness canary."""

import argparse
import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_evaluation as app_eval
from . import efficient_backtest
from . import app_server_judge_v5_selection_v203_fresh_exhaustive_design as v203
from . import app_server_judge_v5_selection_v219_feasibility_blocker as v219
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .paths import db_path
from .util import now_iso, sha256_text, stable_id


V220_SELECTION_VERSION = "pif_app_server_fresh_integrated_selection_v1"
V220_DESIGN_VERSION = "pif_app_server_judge_v5_4_selection_v220_design_v1"
V220_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v220_spec_v1"
V220_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v220_runtime_lock_v1"
)
V220_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v220_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v220_fresh_integrated_base_design"
SEED = "pif-v220-fresh-integrated-base-v1"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
EPISODE_COUNT = 4
SEGMENTS_PER_EPISODE = 2
NO_SIGNAL_PER_EPISODE = 1
DENSE_PER_EPISODE = 1
DENSE_EVENT_MIN = 16
MAX_EVENTS_PER_SEGMENT = 32
WINDOW_COUNT = 4
CONTEXT_CHARS = 900
TURN_COUNT = 4
MAXIMUM_BASE_TOTAL_TOKENS_PER_TURN = 35_000
MAXIMUM_FOLLOWUP_TOTAL_TOKENS_PER_EPISODE = 20_000
BASELINE_END_TO_END_TOKENS = 10_065_426
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = 600_538
BASELINE_SEGMENT_SCOPE = 60
PUBLISH_CUTOFF = "2025-01-01T00:00:00+00:00"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT
    / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v220-fresh-integrated-base-design"
).resolve()

CURRENT_DEVELOPMENT_MANIFEST = (
    PROJECT_ROOT / "work/app-server-development-v2/manifest.json"
).resolve()
HOLDOUT_MANIFESTS = (
    (
        PROJECT_ROOT
        / "work/efficiency-windowed-final-holdout2-20260711/manifest.json"
    ).resolve(),
    (
        PROJECT_ROOT
        / "work/windowed-acceptance-v1/paired-holdout/manifest.json"
    ).resolve(),
    (
        PROJECT_ROOT
        / "work/windowed-acceptance-v1/paired-holdout-v2/manifest.json"
    ).resolve(),
)
V203_MANIFEST = (v203.DEFAULT_OUTPUT_ROOT / "manifest-v3.json").resolve()
EXCLUSION_MANIFESTS = (
    CURRENT_DEVELOPMENT_MANIFEST,
    *HOLDOUT_MANIFESTS,
    V203_MANIFEST,
)
SOURCE_EXCLUSION_MANIFESTS = (
    CURRENT_DEVELOPMENT_MANIFEST,
    V203_MANIFEST,
)


class JudgeV5SelectionV220Error(RuntimeError):
    """The fresh integrated canary design cannot be frozen safely."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text_record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise JudgeV5SelectionV220Error(f"frozen {path.name} drifted")
        return
    path.write_text(value, encoding="utf-8")


def _validate_v219_checkpoint() -> dict[str, Any]:
    root = v219.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "attempt-spec.json",
        "report": root / "full-coverage-feasibility-report.json",
        "audit": root / "full-coverage-feasibility.private.json",
        "runtime_lock": root / "runtime-lock.json",
        "terminal": root / "terminal.json",
    }
    expected = {path.resolve() for path in paths.values()}
    actual = {path.resolve() for path in root.iterdir() if path.is_file()}
    if actual != expected:
        raise JudgeV5SelectionV220Error("v219 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV220Error("v219 immutable artifact drifted")
    v219.verify_runtime_lock(paths["runtime_lock"])
    terminal = _load_json(paths["terminal"], "v219 terminal")
    report = _load_json(paths["report"], "v219 report")
    if (
        terminal.get("state") != "waiting_for_external_authorization"
        or terminal.get("terminal_reason")
        != "fresh_integrated_extraction_validation_authorization_required"
        or terminal.get("v218_canary_quality_and_cost_passed") is not True
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("new_semantic_model_calls") != 0
        or report.get("fixed_bundle_count") != 26
        or report.get("fixed_quality_pass_count_under_perfect_support_selector")
        != 0
        or report.get("required_strategy")
        != (
            "emit_support_and_completeness_gap_routing_inside_one_fresh_base_"
            "extraction_turn_then_run_extra_arms_only_for_observable_gaps"
        )
    ):
        raise JudgeV5SelectionV220Error("v219 checkpoint contract drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "report": report,
    }


def _excluded_source_ids(paths: Sequence[Path]) -> set[str]:
    result: set[str] = set()
    for path in paths:
        payload = _load_json(path, f"source exclusion {path.name}")
        for episode in payload.get("episodes") or []:
            source_id = (
                episode.get("source_id")
                if isinstance(episode, Mapping)
                else None
            )
            if isinstance(source_id, str) and source_id:
                result.add(source_id)
    return result


def _event_count(output_json: str) -> int:
    try:
        value = json.loads(output_json)
    except json.JSONDecodeError as exc:
        raise JudgeV5SelectionV220Error("fresh label output is malformed") from exc
    events = value.get("discourse_events") if isinstance(value, Mapping) else None
    if not isinstance(events, list):
        raise JudgeV5SelectionV220Error("fresh label output lacks discourse events")
    return len(events)


def select_fresh_development_episodes(
    conn: sqlite3.Connection,
    *,
    exclusion_manifest_paths: Sequence[Path] = EXCLUSION_MANIFESTS,
    source_exclusion_manifest_paths: Sequence[Path] = SOURCE_EXCLUSION_MANIFESTS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    excluded_episode_ids, excluded_text_hashes, provenance = (
        app_eval._load_exclusion_manifests(list(exclusion_manifest_paths))
    )
    excluded_sources = _excluded_source_ids(source_exclusion_manifest_paths)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        WITH ranked AS (
          SELECT ecr.episode_id, e.source_id, e.published_at,
                 s.id AS segment_id, s.text_sha256, l.output_json,
                 ROW_NUMBER() OVER (
                   PARTITION BY s.id ORDER BY l.created_at DESC, l.id DESC
                 ) AS label_rank
          FROM episode_context_runs ecr
          JOIN episodes e ON e.id = ecr.episode_id
          JOIN segments s
            ON s.episode_id = ecr.episode_id
           AND s.transcript_id = ecr.transcript_id
          JOIN labels l ON l.segment_id = s.id
          WHERE ecr.label_pack = 'ai_discourse_v3_1'
            AND ecr.model = 'gpt-5.5'
            AND ecr.status = 'completed'
            AND l.label_pack = 'ai_discourse_v3_1'
            AND l.model = 'gpt-5.5'
            AND l.status IN ('ready', 'completed')
        )
        SELECT episode_id, source_id, published_at, segment_id,
               text_sha256, output_json
        FROM ranked
        WHERE label_rank = 1
        """
    ).fetchall()
    counts_by_episode: dict[str, list[int]] = defaultdict(list)
    metadata: dict[str, dict[str, str]] = {}
    for row in rows:
        episode_id = str(row["episode_id"])
        source_id = str(row["source_id"])
        published_at = str(row["published_at"] or "")
        text_hash = str(row["text_sha256"] or "")
        if (
            episode_id in excluded_episode_ids
            or source_id in excluded_sources
            or text_hash in excluded_text_hashes
            or not published_at
            or published_at >= PUBLISH_CUTOFF
        ):
            continue
        counts_by_episode[episode_id].append(_event_count(str(row["output_json"])))
        metadata[episode_id] = {
            "episode_id": episode_id,
            "source_id": source_id,
            "published_at": published_at,
        }
    eligible = []
    for episode_id, counts in counts_by_episode.items():
        no_signal = sum(count == 0 for count in counts)
        dense = sum(count >= DENSE_EVENT_MIN for count in counts)
        observed_max = max(counts, default=0)
        if no_signal < 1 or dense < 1 or observed_max > MAX_EVENTS_PER_SEGMENT:
            continue
        eligible.append(
            {
                **metadata[episode_id],
                "no_signal_label_count": no_signal,
                "dense_label_count": dense,
                "observed_label_event_max": observed_max,
                "labeled_segment_count": len(counts),
            }
        )
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in eligible:
        by_source[str(row["source_id"])].append(row)
    one_per_source = [
        min(
            candidates,
            key=lambda row: (
                sha256_text(
                    f"{SEED}:episode:{source_id}:{row['episode_id']}"
                ),
                str(row["episode_id"]),
            ),
        )
        for source_id, candidates in by_source.items()
    ]
    selected = sorted(
        one_per_source,
        key=lambda row: (
            sha256_text(f"{SEED}:source:{row['source_id']}"),
            str(row["source_id"]),
        ),
    )[:EPISODE_COUNT]
    if (
        len(selected) != EPISODE_COUNT
        or len({row["episode_id"] for row in selected}) != EPISODE_COUNT
        or len({row["source_id"] for row in selected}) != EPISODE_COUNT
    ):
        raise JudgeV5SelectionV220Error(
            "fresh integrated development cohort is insufficient"
        )
    audit = {
        "schema_version": V220_SELECTION_VERSION,
        "seed": SEED,
        "selection_policy": (
            "completed_context_bound_transcripts_and_prior_llm_label_counts_only;"
            "exclude_prior_episode_ids_text_hashes_and_sources;publish_before_2025;"
            "hash_rank_one_episode_per_source"
        ),
        "publish_cutoff_exclusive": PUBLISH_CUTOFF,
        "semantic_regex_or_keyword_rules_used": False,
        "embeddings_or_semantic_similarity_used": False,
        "excluded_episode_count": len(excluded_episode_ids),
        "excluded_text_hash_count": len(excluded_text_hashes),
        "excluded_source_count": len(excluded_sources),
        "eligible_episode_count": len(eligible),
        "eligible_source_count": len(by_source),
        "selected_episode_count": len(selected),
        "selected_source_count": len({row["source_id"] for row in selected}),
        "selected": selected,
        "exclusion_provenance": provenance,
        "privacy": "sanitized_ids_counts_hashes_and_dates_no_transcript_or_event_text",
    }
    return selected, audit


def integrated_episode_batch_schema(
    *, episode_id: str, segment_ids: Sequence[str]
) -> dict[str, Any]:
    schema = app_eval.episode_batch_core_schema(
        episode_id=episode_id,
        segment_ids=list(segment_ids),
        max_events_per_segment=MAX_EVENTS_PER_SEGMENT,
    )
    segment = schema["properties"]["segments"]["items"]
    receipt = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "coverage_status",
            "followup_required",
            "gap_event_types",
            "gap_evidence",
            "rationale",
        ],
        "properties": {
            "coverage_status": {
                "type": "string",
                "enum": [
                    "complete",
                    "material_gaps",
                    "no_eligible_events",
                    "abstain",
                ],
            },
            "followup_required": {"type": "boolean"},
            "gap_event_types": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": list(efficient_backtest.WINDOWED_EVENT_TYPES),
                },
                "maxItems": 8,
            },
            "gap_evidence": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": 4,
            },
            "rationale": {"type": "string"},
        },
    }
    segment["required"].append("coverage_receipt")
    segment["properties"]["coverage_receipt"] = receipt
    return schema


def integrated_core_instructions() -> tuple[str, str]:
    core, guideline_sha = app_eval.episode_batch_core_instructions(
        guideline_path=efficient_backtest.DEFAULT_WINDOWED_GUIDELINES_PATH,
        max_events_per_segment=MAX_EVENTS_PER_SEGMENT,
    )
    addition = """
# Integrated completeness receipt
After drafting every event, perform one fresh source-wide omission pass before
answering. If you find another eligible, independently useful proposition, add it
to events now. Then return one coverage_receipt inside every segment object.

Use complete only when the final events represent every material eligible
proposition you can support from the segment. Use material_gaps only when a
material proposition remains unresolved because the event cap, ambiguous
boundaries, or conflicting attribution prevents a safe event. For material_gaps,
set followup_required=true and return the omitted proposition's event family and
one to four smallest exact source spans in gap_evidence. Do not use a count
heuristic or verbal confidence. Use no_eligible_events only when events is empty
because the source contains no eligible proposition. Use abstain only when source
corruption prevents the completeness decision; set followup_required=true. For
complete and no_eligible_events, set followup_required=false and return empty
gap_event_types and gap_evidence. This receipt is an LLM semantic decision; never
invent evidence or infer hidden reference labels.
""".strip()
    return core + "\n\n" + addition, guideline_sha


def _request_fingerprints(
    conn: sqlite3.Connection,
    *,
    manifest_path: Path,
) -> tuple[list[dict[str, Any]], str]:
    manifest = _load_json(manifest_path, "v220 manifest")
    episodes = app_eval._load_prepared_episodes(
        conn,
        manifest=manifest,
        window_count=WINDOW_COUNT,
        context_chars=CONTEXT_CHARS,
    )
    core, guideline_sha = integrated_core_instructions()
    rows = []
    for episode in episodes:
        segments = list(episode["segments"])
        if len(segments) != SEGMENTS_PER_EPISODE:
            raise JudgeV5SelectionV220Error("v220 episode batch size drifted")
        segment_ids = [str(row["segment_id"]) for row in segments]
        base = app_eval.build_episode_base_instructions(
            core_instructions=core,
            episode=episode,
            episode_context=episode["episode_context"],
        )
        prompt = app_eval.build_episode_batch_prompt(segments=segments)
        schema = integrated_episode_batch_schema(
            episode_id=str(episode["episode_id"]),
            segment_ids=segment_ids,
        )
        schema_text = json.dumps(
            schema, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        rows.append(
            {
                "turn_name": stable_id(
                    PHASE_ID,
                    str(episode["episode_id"]),
                    *segment_ids,
                    prefix="v220_turn_",
                ),
                "episode_id": str(episode["episode_id"]),
                "segment_ids": segment_ids,
                "base_instructions_sha256": sha256_text(base),
                "base_instructions_bytes": len(base.encode("utf-8")),
                "prompt_sha256": sha256_text(prompt),
                "prompt_bytes": len(prompt.encode("utf-8")),
                "schema_sha256": sha256_text(schema_text),
                "schema_bytes": len(schema_text.encode("utf-8")),
                "total_request_bytes": len(base.encode("utf-8"))
                + len(prompt.encode("utf-8"))
                + len(schema_text.encode("utf-8")),
            }
        )
    if len(rows) != TURN_COUNT or len({row["turn_name"] for row in rows}) != TURN_COUNT:
        raise JudgeV5SelectionV220Error("v220 turn plan drifted")
    return rows, guideline_sha


def _cost_bound() -> dict[str, Any]:
    base = TURN_COUNT * MAXIMUM_BASE_TOTAL_TOKENS_PER_TURN
    followup = EPISODE_COUNT * MAXIMUM_FOLLOWUP_TOTAL_TOKENS_PER_EPISODE
    scaled_base = math.ceil(
        base
        * BASELINE_SEGMENT_SCOPE
        / (EPISODE_COUNT * SEGMENTS_PER_EPISODE)
    )
    scaled_joint = math.ceil(
        (base + followup)
        * BASELINE_SEGMENT_SCOPE
        / (EPISODE_COUNT * SEGMENTS_PER_EPISODE)
    )
    base_total = scaled_base + PRODUCTION_AMORTIZED_CONTEXT_TOKENS
    joint_total = scaled_joint + PRODUCTION_AMORTIZED_CONTEXT_TOKENS
    return {
        "development_segment_count": EPISODE_COUNT * SEGMENTS_PER_EPISODE,
        "declared_base_turn_count": TURN_COUNT,
        "maximum_base_total_tokens_per_turn": MAXIMUM_BASE_TOTAL_TOKENS_PER_TURN,
        "maximum_base_development_tokens": base,
        "maximum_followup_total_tokens_per_episode": (
            MAXIMUM_FOLLOWUP_TOTAL_TOKENS_PER_EPISODE
        ),
        "maximum_followup_development_tokens": followup,
        "base_production_amortized_total_tokens": base_total,
        "base_production_amortized_total_token_ratio": round(
            base_total / BASELINE_END_TO_END_TOKENS, 6
        ),
        "base_plus_followup_production_amortized_total_tokens": joint_total,
        "base_plus_followup_production_amortized_total_token_ratio": round(
            joint_total / BASELINE_END_TO_END_TOKENS, 6
        ),
        "base_plus_followup_passes_lte_0_28": (
            joint_total / BASELINE_END_TO_END_TOKENS <= 0.28
        ),
    }


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v219._expected_runtime_paths())
            | {
                Path(__file__).resolve(),
                Path(v203.__file__).resolve(),
                Path(app_eval.__file__).resolve(),
                Path(efficient_backtest.__file__).resolve(),
                efficient_backtest.DEFAULT_WINDOWED_GUIDELINES_PATH.resolve(),
            },
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    predecessor: Mapping[str, Any],
    spec_path: Path,
    design_path: Path,
    manifest_path: Path,
    selection_path: Path,
    request_path: Path,
    instructions_path: Path,
) -> Path:
    path = root / "runtime-lock.json"
    manifest = _load_json(manifest_path, "v220 manifest")
    records = {
        "spec": _record(spec_path),
        "design": _record(design_path),
        "manifest": _record(manifest_path),
        "selection": _record(selection_path),
        "request_fingerprints": _record(request_path),
        "instructions": _text_record(instructions_path),
        "shared_reference_seed": _record(
            Path(manifest["shared_reference_seed"]["artifact_path"])
        ),
        "reference_noise": _record(
            Path(manifest["reference_noise"]["artifact_path"])
        ),
    }
    lock = {
        "schema_version": V220_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "predecessor_v219": list(predecessor["records"].values()),
        "exclusion_manifests": [_record(item) for item in EXCLUSION_MANIFESTS],
        "frozen_inputs": records,
        "fresh_nonreplay_extraction_only": True,
        "extraction_rerun_allowed": False,
        "batch_5_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_immutable(path, lock)
    verify_runtime_lock(path)
    return path


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v220 runtime lock")
    predecessor = _validate_v219_checkpoint()
    expected_runtime = {str(item) for item in _expected_runtime_paths()}
    actual_runtime = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping) and isinstance(record.get("path"), str)
    }
    expected_exclusions = [_record(item) for item in EXCLUSION_MANIFESTS]
    if (
        lock.get("schema_version") != V220_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or actual_runtime != expected_runtime
        or lock.get("predecessor_v219")
        != list(predecessor["records"].values())
        or lock.get("exclusion_manifests") != expected_exclusions
        or lock.get("fresh_nonreplay_extraction_only") is not True
        or lock.get("extraction_rerun_allowed") is not False
        or lock.get("batch_5_replay_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV220Error("v220 runtime lock drifted")
    records = [
        *(lock.get("runtime_files") or []),
        *(lock.get("predecessor_v219") or []),
        *(lock.get("exclusion_manifests") or []),
        *(lock.get("frozen_inputs") or {}).values(),
    ]
    if any(not _verify_record(record) for record in records):
        raise JudgeV5SelectionV220Error("v220 runtime lock record drifted")
    manifest = _load_json(
        Path(lock["frozen_inputs"]["manifest"]["path"]), "v220 manifest"
    )
    excluded_episode_ids, excluded_text_hashes, _ = (
        app_eval._load_exclusion_manifests(list(EXCLUSION_MANIFESTS))
    )
    selected_episodes = {
        str(episode["episode_id"]) for episode in manifest.get("episodes") or []
    }
    selected_hashes = {
        str(segment["text_sha256"])
        for episode in manifest.get("episodes") or []
        for segment in episode.get("segments") or []
    }
    if (
        selected_episodes & excluded_episode_ids
        or selected_hashes & excluded_text_hashes
    ):
        raise JudgeV5SelectionV220Error("v220 manifest overlaps excluded evidence")
    return lock


def freeze_v220(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Optional[Path] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v220 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV220Error("v220 root is nonempty without a terminal")
    predecessor = _validate_v219_checkpoint()
    source_db = (database_path or db_path()).expanduser().resolve()
    conn = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        selected, selection = select_fresh_development_episodes(conn)
        selection_path = root / "fresh-development-selection-audit.json"
        _write_immutable(selection_path, selection)
        manifest_path = root / "manifest-v4.json"
        exported = app_eval.export_app_server_development_manifest_v2(
            conn,
            output_path=manifest_path,
            episode_ids=[str(row["episode_id"]) for row in selected],
            exclusion_manifest_paths=list(EXCLUSION_MANIFESTS),
            segments_per_episode=SEGMENTS_PER_EPISODE,
            minimum_no_signal_per_episode=NO_SIGNAL_PER_EPISODE,
            maximum_no_signal_per_episode=NO_SIGNAL_PER_EPISODE,
            minimum_dense_per_episode=DENSE_PER_EPISODE,
            dense_event_min=DENSE_EVENT_MIN,
            event_cap=MAX_EVENTS_PER_SEGMENT,
            seed=SEED,
        )
        manifest = _load_json(manifest_path, "v220 manifest")
        request_rows, guideline_sha = _request_fingerprints(
            conn, manifest_path=manifest_path
        )
    finally:
        conn.close()
    if (
        exported.get("episode_count") != EPISODE_COUNT
        or exported.get("source_count") != EPISODE_COUNT
        or exported.get("segment_count") != EPISODE_COUNT * SEGMENTS_PER_EPISODE
        or exported.get("unique_text_sha256_count")
        != EPISODE_COUNT * SEGMENTS_PER_EPISODE
        or exported.get("density_counts") != {"dense": 4, "no_signal": 4}
        or manifest.get("event_cap") != MAX_EVENTS_PER_SEGMENT
    ):
        raise JudgeV5SelectionV220Error("v220 manifest invariants failed")
    core, observed_guideline_sha = integrated_core_instructions()
    if observed_guideline_sha != guideline_sha:
        raise JudgeV5SelectionV220Error("v220 guideline hash drifted")
    instructions_path = root / "integrated-core-instructions.private.md"
    _write_private_text(instructions_path, core)
    request_path = root / "request-fingerprints.private.json"
    _write_immutable(
        request_path,
        {
            "schema_version": "pif_app_server_v220_request_fingerprints_v1",
            "turns": request_rows,
            "guideline_sha256": guideline_sha,
            "privacy": "hashes_ids_and_sizes_only_no_source_or_event_text",
        },
    )
    cost = _cost_bound()
    if cost["base_plus_followup_passes_lte_0_28"] is not True:
        raise JudgeV5SelectionV220Error("v220 declared cost bound exceeds target")
    design = {
        "schema_version": V220_DESIGN_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "fresh_nonreplay_integrated_extraction_and_completeness_receipt",
        "hypothesis": (
            "the model can perform extraction and omission routing while the full "
            "source is "
            "already resident, avoiding a second full-source semantic prefill"
        ),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "episode_count": EPISODE_COUNT,
        "segments_per_episode": SEGMENTS_PER_EPISODE,
        "density_counts": {"dense": 4, "no_signal": 4},
        "declared_turn_count": TURN_COUNT,
        "thread_mode": "new_thread",
        "concurrency": 1,
        "retry_count_per_turn": 0,
        "window_count": WINDOW_COUNT,
        "context_chars": CONTEXT_CHARS,
        "max_events_per_segment": MAX_EVENTS_PER_SEGMENT,
        "fresh_development_only": True,
        "prior_episode_or_text_replayed": False,
        "extraction_rerun_allowed": False,
        "batch_5_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "semantic_decisions_owned_by_llm": [
            "event_discovery",
            "evidence",
            "actor_and_attribution",
            "claim_and_event_type",
            "source_context",
            "completeness_status",
            "gap_event_types",
            "gap_evidence",
        ],
        "deterministic_scope": [
            "schema_and_enum_validation",
            "exact_evidence_and_offsets",
            "literal_metric_grounding",
            "identity_order_and_provenance",
            "caps_lifecycle_and_accounting",
        ],
        "cost_bound": cost,
        "promotion_gates": {
            "all_8_segments_validated": True,
            "all_4_turns_usage_measured": True,
            "normalized_exact_evidence_rate": 1.0,
            "no_signal_candidate_positive_segments": 0,
            "metric_grounding_error_events": 0,
            "dense_median_candidate_to_reference_event_count_ratio_min": 0.75,
            "unflagged_dense_coverage_shortfall_count": 0,
            "coverage_receipt_contract_valid": True,
            "base_production_amortized_total_token_ratio_max": 0.18,
            "base_plus_bounded_followup_total_token_ratio_max": 0.28,
        },
        "semantic_quality_scored_in_this_phase": False,
        "fresh_frozen_judge_authorized_only_after_structural_gate": True,
    }
    design_path = root / "integrated-base-design.json"
    _write_immutable(design_path, design)
    spec = {
        "schema_version": V220_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_semantic_attempt",
        "semantic_model_calls_declared": TURN_COUNT,
        "semantic_model_calls_started": 0,
        "fresh_nonreplay_extraction_authorized": True,
        "extraction_rerun_authorized": False,
        "batch_5_replay_authorized": False,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "api_key_billing_allowed": False,
        "raw_session_token_access_allowed": False,
        "codex_exec_semantic_calls_allowed": False,
        "retry_count_per_turn": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "predecessor_v219": predecessor["records"],
        "exclusion_manifests": [_record(item) for item in EXCLUSION_MANIFESTS],
        "frozen_inputs": {
            "selection": _record(selection_path),
            "manifest": _record(manifest_path),
            "shared_reference_seed": _record(
                Path(manifest["shared_reference_seed"]["artifact_path"])
            ),
            "reference_noise": _record(
                Path(manifest["reference_noise"]["artifact_path"])
            ),
            "request_fingerprints": _record(request_path),
            "instructions": _text_record(instructions_path),
            "design": _record(design_path),
        },
    }
    spec_path = root / "attempt-spec.json"
    _write_immutable(spec_path, spec)
    runtime_lock = _freeze_runtime_lock(
        root=root,
        predecessor=predecessor,
        spec_path=spec_path,
        design_path=design_path,
        manifest_path=manifest_path,
        selection_path=selection_path,
        request_path=request_path,
        instructions_path=instructions_path,
    )
    terminal = {
        "schema_version": V220_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v220_fresh_nonreplay_integrated_base_canary_authorized",
        "terminal_classification": "active_development_recovery_required",
        "overall_evaluation_complete": False,
        "semantic_attempt_authorized": True,
        "authorized_turn_count": TURN_COUNT,
        "authorized_model": MODEL,
        "authorized_effort": EFFORT,
        "fresh_nonreplay_extraction_authorized": True,
        "extraction_rerun_authorized": False,
        "batch_5_replayed": False,
        "prior_episode_or_text_replayed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_conservative_unknown_usage_upper_bound": predecessor[
            "terminal"
        ]["cumulative_conservative_unknown_usage_upper_bound"],
        "selection_audit": _record(selection_path),
        "manifest": _record(manifest_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "runtime_lock": _record(runtime_lock),
        "required_next_artifact_path": str(
            PIPELINE_ROOT
            / "development-selection-v5_4-v221-fresh-integrated-base-canary"
            / "terminal.json"
        ),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Freeze the v220 fresh integrated base canary design"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--database", default=str(db_path()))
    args = parser.parse_args(argv)
    terminal = freeze_v220(
        output_dir=Path(args.output_dir), database_path=Path(args.database)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "semantic_attempt_authorized": terminal[
                    "semantic_attempt_authorized"
                ],
                "fresh_nonreplay_extraction_authorized": terminal[
                    "fresh_nonreplay_extraction_authorized"
                ],
                "extraction_rerun_authorized": terminal[
                    "extraction_rerun_authorized"
                ],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
