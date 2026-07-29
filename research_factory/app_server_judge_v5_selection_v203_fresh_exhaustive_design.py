from __future__ import annotations

"""Freeze a non-replay, high-reasoning extraction diagnostic on fresh development data."""

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_evaluation as app_eval
from . import app_server_judge_v5_selection_v202_extraction_quality_nonacceptance as v202
from . import efficient_backtest
from .app_server_evaluation import (
    DEFAULT_WINDOWED_GUIDELINES_PATH,
    _load_exclusion_manifests,
    export_app_server_development_manifest_v2,
)
from .app_server_judge_v5_calibration_v26_diagnostic import _load_json, _write_immutable
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .paths import db_path
from .util import now_iso, sha256_text


V203_DESIGN_VERSION = "pif_app_server_judge_v5_4_selection_v203_design_v1"
V203_AUDIT_VERSION = "pif_app_server_fresh_development_selection_audit_v1"
V203_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v203_spec_v1"
V203_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v203_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v203_fresh_exhaustive_design"
SEED = "pif-v203-fresh-exhaustive-dev-v1"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
BATCH_SIZE = 4
THREAD_MODE = "new_thread"
EPISODE_COUNT = 4
SEGMENTS_PER_EPISODE = 4
NO_SIGNAL_PER_EPISODE = 1
DENSE_PER_EPISODE = 3
DENSE_EVENT_MIN = 16
MAX_EVENTS_PER_SEGMENT = 32
TURN_COUNT = 4
MAXIMUM_TOTAL_TOKENS_PER_TURN = 140000
BASELINE_END_TO_END_TOKENS = 10065426
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = 600538
BASELINE_SEGMENT_SCOPE = 60
DEFAULT_OUTPUT_ROOT = (
    v202.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v203-authorized-extraction-quality-strategy"
).resolve()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXCLUSION_MANIFESTS = (
    (PROJECT_ROOT / "work/app-server-development-v2/manifest.json").resolve(),
    (
        PROJECT_ROOT
        / "work/efficiency-windowed-final-holdout2-20260711/manifest.json"
    ).resolve(),
    (
        PROJECT_ROOT / "work/windowed-acceptance-v1/paired-holdout/manifest.json"
    ).resolve(),
    (
        PROJECT_ROOT / "work/windowed-acceptance-v1/paired-holdout-v2/manifest.json"
    ).resolve(),
)


class JudgeV5SelectionV203Error(RuntimeError):
    """The fresh extraction diagnostic cannot be authorized safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v202_nonacceptance() -> dict[str, Any]:
    root = v202.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "report": root / "development-nonacceptance-report.json",
        "spec": root / "development-nonacceptance-spec.json",
    }
    values = {name: _load_json(path, f"v202 {name}") for name, path in paths.items()}
    terminal, report, spec = values["terminal"], values["report"], values["spec"]
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    if (
        terminal.get("state") != "waiting_for_extraction_quality_strategy_authorization"
        or terminal.get("terminal_reason")
        != "development_quality_target_not_met_token_target_met"
        or terminal.get("terminal_classification")
        != "external_policy_authorization_required"
        or terminal.get("development_quality_passed") is not False
        or terminal.get("production_amortized_token_target_passed") is not True
        or terminal.get("production_amortized_total_token_ratio") != 0.236727
        or terminal.get("safe_local_judge_only_experiment_remaining") is not False
        or terminal.get("extraction_rerun_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage") != zero_usage
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8708241
        or terminal.get("cumulative_unknown_usage_turn_count") != 2
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 220000
        or terminal.get("nonacceptance_report") != _record(paths["report"])
        or terminal.get("spec") != _record(paths["spec"])
        or report.get("blocker_class")
        != "measured_development_extraction_quality_shortfall"
        or report.get("semantic_quality_passed") is not False
        or report.get("token_target_passed") is not True
        or report.get("safe_local_judge_only_experiment_remaining") is not False
        or spec.get("state") != "zero_token_nonacceptance_receipt_completed"
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("extraction_model_calls_started") != 0
        or spec.get("holdout_authorized") is not False
    ):
        raise JudgeV5SelectionV203Error("v202 nonacceptance contract drifted")
    for record in [
        *spec.get("runtime_files", []),
        *spec.get("frozen_inputs", {}).values(),
        spec.get("report"),
    ]:
        if not isinstance(record, Mapping) or not _verify_record(record):
            raise JudgeV5SelectionV203Error("v202 frozen binding drifted")
    v202._validate_v201_nonacceptance()
    return {
        "root": root,
        "terminal": terminal,
        "report": report,
        "spec": spec,
        "records": {name: _record(path) for name, path in paths.items()},
    }


def _event_count(output_json: str) -> int:
    value = json.loads(output_json)
    events = value.get("discourse_events") if isinstance(value, dict) else None
    if not isinstance(events, list):
        raise JudgeV5SelectionV203Error("fresh development label output is malformed")
    return len(events)


def select_fresh_development_episodes(
    conn: sqlite3.Connection,
    *,
    exclusion_manifest_paths: Sequence[Path] = EXCLUSION_MANIFESTS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    excluded_episode_ids, excluded_text_hashes, exclusion_provenance = (
        _load_exclusion_manifests(list(exclusion_manifest_paths))
    )
    current_manifest = _load_json(EXCLUSION_MANIFESTS[0], "current development manifest")
    prior_development_sources = {
        str(row["source_id"]) for row in current_manifest.get("episodes") or []
    }
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
            ON s.episode_id = ecr.episode_id AND s.transcript_id = ecr.transcript_id
          JOIN labels l ON l.segment_id = s.id
          WHERE ecr.label_pack = 'ai_discourse_v3_1'
            AND ecr.model = 'gpt-5.5'
            AND ecr.status = 'completed'
            AND l.label_pack = 'ai_discourse_v3_1'
            AND l.model = 'gpt-5.5'
            AND l.status IN ('ready', 'completed')
        )
        SELECT episode_id, source_id, published_at, segment_id, text_sha256, output_json
        FROM ranked
        WHERE label_rank = 1
        """
    ).fetchall()
    by_episode: dict[str, list[int]] = defaultdict(list)
    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        episode_id = str(row["episode_id"])
        source_id = str(row["source_id"])
        text_hash = str(row["text_sha256"] or "")
        if (
            episode_id in excluded_episode_ids
            or source_id in prior_development_sources
            or text_hash in excluded_text_hashes
        ):
            continue
        by_episode[episode_id].append(_event_count(str(row["output_json"])))
        metadata[episode_id] = {
            "episode_id": episode_id,
            "source_id": source_id,
            "published_at": str(row["published_at"] or ""),
        }
    eligible = []
    for episode_id, counts in by_episode.items():
        no_signal_count = sum(count == 0 for count in counts)
        dense_count = sum(count >= DENSE_EVENT_MIN for count in counts)
        observed_max = max(counts, default=0)
        if (
            no_signal_count < NO_SIGNAL_PER_EPISODE
            or dense_count < DENSE_PER_EPISODE
            or observed_max > MAX_EVENTS_PER_SEGMENT
        ):
            continue
        eligible.append(
            {
                **metadata[episode_id],
                "no_signal_label_count": no_signal_count,
                "dense_label_count": dense_count,
                "observed_label_event_max": observed_max,
                "labeled_segment_count": len(counts),
            }
        )
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in eligible:
        by_source[str(row["source_id"])].append(row)
    one_per_source = []
    for source_id, candidates in by_source.items():
        one_per_source.append(
            min(
                candidates,
                key=lambda row: (
                    sha256_text(
                        f"{SEED}:episode:{source_id}:{row['episode_id']}"
                    ),
                    str(row["episode_id"]),
                ),
            )
        )
    selected = sorted(
        one_per_source,
        key=lambda row: (
            sha256_text(f"{SEED}:source:{row['source_id']}"),
            str(row["source_id"]),
        ),
    )[:EPISODE_COUNT]
    if (
        len(selected) != EPISODE_COUNT
        or len({row["source_id"] for row in selected}) != EPISODE_COUNT
        or len({row["episode_id"] for row in selected}) != EPISODE_COUNT
    ):
        raise JudgeV5SelectionV203Error("fresh development cohort is insufficient")
    audit = {
        "schema_version": V203_AUDIT_VERSION,
        "seed": SEED,
        "selection_policy": (
            "completed_context_bound_transcripts_and_prior_llm_label_counts_only; "
            "exclude_prior_episode_ids_and_text_hashes; hash_rank_one_episode_per_source"
        ),
        "semantic_regex_or_keyword_rules_used": False,
        "embeddings_or_semantic_similarity_used": False,
        "prior_development_source_count_excluded": len(prior_development_sources),
        "excluded_episode_count": len(excluded_episode_ids),
        "excluded_text_hash_count": len(excluded_text_hashes),
        "eligible_episode_count": len(eligible),
        "eligible_source_count": len(by_source),
        "selected_episode_count": len(selected),
        "selected_source_count": len({row["source_id"] for row in selected}),
        "selected": selected,
        "exclusion_provenance": exclusion_provenance,
        "privacy": "sanitized_ids_counts_hashes_and_dates_no_transcript_or_event_text",
    }
    return selected, audit


def _cost_bound() -> dict[str, Any]:
    measured_turn_bound = TURN_COUNT * MAXIMUM_TOTAL_TOKENS_PER_TURN
    scaled_extraction = measured_turn_bound * BASELINE_SEGMENT_SCOPE // (
        EPISODE_COUNT * SEGMENTS_PER_EPISODE
    )
    numerator = scaled_extraction + PRODUCTION_AMORTIZED_CONTEXT_TOKENS
    ratio = round(numerator / BASELINE_END_TO_END_TOKENS, 6)
    return {
        "declared_turn_count": TURN_COUNT,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "development_measured_token_bound": measured_turn_bound,
        "development_segment_count": EPISODE_COUNT * SEGMENTS_PER_EPISODE,
        "scaled_extraction_tokens_to_60_segments": scaled_extraction,
        "production_amortized_context_tokens": PRODUCTION_AMORTIZED_CONTEXT_TOKENS,
        "baseline_end_to_end_tokens": BASELINE_END_TO_END_TOKENS,
        "production_amortized_candidate_token_bound": numerator,
        "production_amortized_total_token_ratio_bound": ratio,
        "passed_lte_0_28": ratio <= 0.28,
    }


def freeze_v203(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Path | None = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v203 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV203Error("v203 root is nonempty without a terminal")
    predecessor = _validate_v202_nonacceptance()
    source_db = (database_path or db_path()).expanduser().resolve()
    conn = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    try:
        selected, selection_audit = select_fresh_development_episodes(conn)
        audit_path = root / "fresh-development-selection-audit.json"
        _write_immutable(audit_path, selection_audit)
        manifest_path = root / "manifest-v3.json"
        export_result = export_app_server_development_manifest_v2(
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
    finally:
        conn.close()
    manifest = _load_json(manifest_path, "v203 manifest")
    reference_path = Path(manifest["shared_reference_seed"]["artifact_path"])
    noise_path = Path(manifest["reference_noise"]["artifact_path"])
    if (
        export_result.get("segment_count") != EPISODE_COUNT * SEGMENTS_PER_EPISODE
        or export_result.get("source_count") != EPISODE_COUNT
        or export_result.get("density_counts")
        != {"dense": EPISODE_COUNT * DENSE_PER_EPISODE, "no_signal": EPISODE_COUNT}
        or export_result.get("observed_golden_event_max", 0) > MAX_EVENTS_PER_SEGMENT
    ):
        raise JudgeV5SelectionV203Error("fresh development manifest invariants failed")
    cost = _cost_bound()
    if cost["passed_lte_0_28"] is not True:
        raise JudgeV5SelectionV203Error("fresh diagnostic token bound exceeds target")
    design = {
        "schema_version": V203_DESIGN_VERSION,
        "state": "frozen_before_semantic_attempt",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "fresh_batch4_high_reasoning_exhaustive_core",
        "hypothesis": (
            "the unchanged topic-neutral prompt underextracts because low reasoning compresses "
            "the full-segment proposition inventory; high reasoning can improve recall in one pass"
        ),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "batch_size": BATCH_SIZE,
        "thread_mode": THREAD_MODE,
        "window_count": 4,
        "context_chars": 900,
        "max_events_per_segment": MAX_EVENTS_PER_SEGMENT,
        "declared_turn_count": TURN_COUNT,
        "retry_count_per_turn": 0,
        "timeout_seconds_per_turn": 1200,
        "guideline_prompt_changed": False,
        "guideline": _record(DEFAULT_WINDOWED_GUIDELINES_PATH),
        "fresh_development_only": True,
        "prior_episode_or_text_replayed": False,
        "batch_5_replayed": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "structural_stop_before_judge": {
            "all_16_segments_validated": True,
            "all_usage_measured": True,
            "normalized_exact_evidence_rate": 1.0,
            "no_signal_candidate_positive_segments": 0,
            "metric_grounding_error_events": 0,
            "dense_median_candidate_to_reference_event_count_ratio_min": 0.75,
            "observed_production_amortized_total_token_ratio_lte": 0.28,
        },
        "semantic_promotion_rule": (
            "structural success authorizes a fresh frozen support-first and neutral-alignment "
            "score; only unchanged semantic noninferiority gates may freeze a winner"
        ),
        "cost_bound": cost,
        "privacy": "private_manifest_and_outputs_sanitized_reports_no_transcript_or_event_text",
    }
    design_path = root / "fresh-exhaustive-design.json"
    _write_stable_time(design_path, design, "created_at")
    spec = {
        "schema_version": V203_SPEC_VERSION,
        "state": "frozen_before_semantic_attempt",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "semantic_model_calls_declared": TURN_COUNT,
        "semantic_model_calls_started": 0,
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "codex_exec_semantic_calls_allowed": False,
        "api_key_billing_allowed": False,
        "raw_session_token_access_allowed": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v202.__file__)),
            _record(Path(app_eval.__file__)),
            _record(Path(efficient_backtest.__file__)),
        ],
        "predecessor": predecessor["records"],
        "exclusion_manifests": [_record(path) for path in EXCLUSION_MANIFESTS],
        "frozen_inputs": {
            "selection_audit": _record(audit_path),
            "manifest": _record(manifest_path),
            "shared_reference_seed": _record(reference_path),
            "reference_noise": _record(noise_path),
            "guideline": _record(DEFAULT_WINDOWED_GUIDELINES_PATH),
            "design": _record(design_path),
        },
    }
    spec_path = root / "fresh-exhaustive-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    terminal = {
        "schema_version": V203_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v203_fresh_exhaustive_diagnostic_frozen_semantic_attempt_authorized",
        "terminal_classification": "active_development_recovery_required",
        "overall_evaluation_complete": False,
        "development_winner_frozen": False,
        "semantic_attempt_authorized": True,
        "authorized_turn_count": TURN_COUNT,
        "authorized_model": MODEL,
        "authorized_effort": EFFORT,
        "prior_episode_or_text_replayed": False,
        "extraction_rerun_authorized": False,
        "batch_5_replayed": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "prospective_shadow_authorized": False,
        "production_mutated": False,
        "semantic_attempt_started": False,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 2,
        "cumulative_conservative_unknown_usage_upper_bound": 220000,
        "production_amortized_total_token_ratio_bound": cost[
            "production_amortized_total_token_ratio_bound"
        ],
        "manifest": _record(manifest_path),
        "selection_audit": _record(audit_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v204-fresh-exhaustive-diagnostic"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v203 fresh extraction design")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--database", default=str(db_path()))
    args = parser.parse_args(argv)
    terminal = freeze_v203(
        output_dir=Path(args.output_dir), database_path=Path(args.database)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "authorized_turn_count": terminal["authorized_turn_count"],
                "prior_episode_or_text_replayed": terminal[
                    "prior_episode_or_text_replayed"
                ],
                "production_amortized_total_token_ratio_bound": terminal[
                    "production_amortized_total_token_ratio_bound"
                ],
                "holdout_authorized": terminal["holdout_authorized"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
