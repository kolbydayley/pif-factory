from __future__ import annotations

"""Run a bounded Luna omission-only pass over the frozen v220 drafts."""

import argparse
import asyncio
import json
import math
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_evaluation as app_eval
from . import app_server_judge_v5 as judge
from . import app_server_judge_v5_selection_v220_integrated_base_design as v220
from . import (
    app_server_judge_v5_selection_v222_integrated_partial_nonacceptance as v222,
)
from . import (
    app_server_judge_v5_selection_v226_alignment_nonacceptance as v226,
)
from . import codex_app_server
from . import efficient_backtest
from .app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .labels import ValidationError, _validate_schema
from .paths import db_path
from .util import now_iso, sha256_text, stable_id


V227_DESIGN_VERSION = "pif_app_server_judge_v5_4_selection_v227_design_v1"
V227_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v227_spec_v1"
V227_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V227_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V227_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v227_runtime_lock_v1"
)
V227_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v227_launch_v1"
V227_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v227_gate_v1"
V227_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v227_failure_v1"
V227_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v227_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v227_luna_completeness"
MODEL = "gpt-5.6-luna"
EFFORT = "low"
EPISODE_COUNT = 2
SEGMENT_COUNT = 4
SEGMENTS_PER_EPISODE = 2
MAX_EVENTS_PER_SEGMENT = 32
MAXIMUM_TOTAL_TOKENS_PER_TURN = 60_000
MAXIMUM_PHASE_TOTAL_TOKENS = 120_000
MAXIMUM_PROMPT_BYTES_PER_TURN = 130_000
MAXIMUM_SCHEMA_BYTES_PER_TURN = 30_000
MAXIMUM_PROMOTION_TOKENS = 77_250
MINIMUM_REMAINING_RESERVE_PERCENT = 20
TIMEOUT_SECONDS = 1200.0
BASELINE_END_TO_END_TOKENS = v220.BASELINE_END_TO_END_TOKENS
BASE_CANDIDATE_PROJECTED_TOKENS = 1_659_553
PRODUCTION_SEGMENT_SCOPE = v220.BASELINE_SEGMENT_SCOPE
OBSERVED_SEGMENT_SCOPE = SEGMENT_COUNT
DEFAULT_OUTPUT_ROOT = (
    v220.PIPELINE_ROOT
    / "development-selection-v5_4-v227-independent-completeness-strategy"
).resolve()


class JudgeV5SelectionV227Error(RuntimeError):
    """The v227 completeness attempt cannot be frozen or adopted safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise JudgeV5SelectionV227Error(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(
    left: Mapping[str, int], right: Mapping[str, int]
) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _predecessor_paths() -> dict[str, Path]:
    v226_root = v226.DEFAULT_OUTPUT_ROOT
    v222_root = v222.DEFAULT_OUTPUT_ROOT
    v220_root = v220.DEFAULT_OUTPUT_ROOT
    return {
        "v226_terminal": v226_root / "terminal.json",
        "v226_report": v226_root / "development-nonacceptance-report.json",
        "v226_spec": v226_root / "attempt-spec.json",
        "v222_terminal": v222_root / "terminal.json",
        "v222_gate": v222_root / "partial-structural-gate.json",
        "v222_private": v222_root / "partial-adoption.private.json",
        "v220_terminal": v220_root / "terminal.json",
        "v220_manifest": v220_root / "manifest-v4.json",
        "v220_design": v220_root / "integrated-base-design.json",
        "v220_spec": v220_root / "attempt-spec.json",
    }


def _validate_predecessor() -> dict[str, Any]:
    paths = _predecessor_paths()
    if any(not path.is_file() for path in paths.values()):
        raise JudgeV5SelectionV227Error("v227 predecessor artifact is missing")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV227Error("v227 predecessor artifact drifted")
    v225_evidence = v226._validate_v225_nonacceptance()
    terminal = _load_json(paths["v226_terminal"], "v226 terminal")
    report = _load_json(paths["v226_report"], "v226 report")
    v222_terminal = _load_json(paths["v222_terminal"], "v222 terminal")
    gate = _load_json(paths["v222_gate"], "v222 gate")
    private = _load_json(paths["v222_private"], "v222 private adoption")
    manifest = _load_json(paths["v220_manifest"], "v220 manifest")
    if (
        terminal.get("state") != "development_strategy_not_accepted"
        or terminal.get("terminal_reason")
        != "v226_joint_quality_cost_strategy_not_accepted"
        or terminal.get("development_quality_passed") is not False
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("production_amortized_token_target_passed") is not True
        or terminal.get("production_amortized_total_token_ratio") != 0.164877
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or report.get("viable_systems_meeting_joint_gates") != []
        or report.get(
            "more_current_strategy_development_cases_can_change_verdict"
        )
        is not False
        or v222_terminal.get("state") != "development_strategy_not_accepted"
        or gate.get("observed_production_amortized_total_tokens")
        != BASE_CANDIDATE_PROJECTED_TOKENS
        or gate.get("observed_production_amortized_total_token_ratio")
        != 0.164877
        or len(private.get("turns") or []) != EPISODE_COUNT
        or len(private.get("cases") or []) != SEGMENT_COUNT
        or manifest.get("schema_version")
        != app_eval.APP_SERVER_DEVELOPMENT_MANIFEST_V2
        or v225_evidence["terminal"].get("development_quality_passed")
        is not False
    ):
        raise JudgeV5SelectionV227Error("v227 predecessor contract drifted")
    return {
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "report": report,
        "gate": gate,
        "private": private,
        "manifest": manifest,
    }


def completeness_core_instructions() -> tuple[str, str]:
    core, guideline_sha = app_eval.episode_batch_core_instructions(
        guideline_path=efficient_backtest.DEFAULT_WINDOWED_GUIDELINES_PATH,
        max_events_per_segment=MAX_EVENTS_PER_SEGMENT,
    )
    override = """
# Independent omission-only pass
Each segment packet contains fixed evidence windows and immutable
existing_events from a prior extractor. Read every word of every window and
semantically compare the source with all existing events. Return only eligible,
independently useful events that are not already represented by an existing
event. Do not copy, delete, rewrite, repair, or relabel an existing event.

Treat a paraphrase or restatement as already represented. Return a new event
only for a material truth-conditional difference such as actor, attribution,
stance, certainty, temporal horizon, metric, negation, causal mechanism, event
boundary, or event type. This decision must come from language understanding,
never keywords, regex, topic rules, event counts, or hidden labels.

For this pass, status describes the omission-only output. Use coded if and only
if at least one new grounded event is returned. Use no_signal with events=[]
when no additional eligible event remains, even when existing_events is
nonempty. Every new event still requires one exact contiguous evidence span and
all normal critical semantic fields. The combined existing and new events must
not exceed the supplied remaining_event_slots ceiling. Return JSON only.
""".strip()
    return core + "\n\n" + override, guideline_sha


def _existing_events_by_segment(
    predecessor: Mapping[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for turn in predecessor["private"]["turns"]:
        for row in turn["normalized"]["segments"]:
            segment_id = str(row["segment_id"])
            if segment_id in result:
                raise JudgeV5SelectionV227Error(
                    "v227 duplicate predecessor segment"
                )
            result[segment_id] = [dict(event) for event in row.get("events") or []]
    if len(result) != SEGMENT_COUNT:
        raise JudgeV5SelectionV227Error("v227 predecessor segment coverage drifted")
    return result


def _gap_prompt(packets: Sequence[Mapping[str, Any]]) -> str:
    return (
        "# Fixed evidence windows and fallible existing events\n"
        + json.dumps(list(packets), ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )


def _load_prepared_turns(
    predecessor: Mapping[str, Any],
    *,
    database_path: Optional[Path] = None,
) -> dict[str, Any]:
    source = (database_path or db_path()).expanduser().resolve()
    conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        episodes = app_eval._load_prepared_episodes(
            conn,
            manifest=predecessor["manifest"],
            window_count=v220.WINDOW_COUNT,
            context_chars=v220.CONTEXT_CHARS,
        )
    finally:
        conn.close()
    existing_by_segment = _existing_events_by_segment(predecessor)
    expected_ids = set(existing_by_segment)
    selected = [
        episode
        for episode in episodes
        if {str(row["segment_id"]) for row in episode["segments"]}
        <= expected_ids
    ]
    if (
        len(selected) != EPISODE_COUNT
        or sum(len(row["segments"]) for row in selected) != SEGMENT_COUNT
        or {
            str(segment["segment_id"])
            for episode in selected
            for segment in episode["segments"]
        }
        != expected_ids
    ):
        raise JudgeV5SelectionV227Error("v227 prepared episode coverage drifted")
    density_by_segment = {
        str(row["segment_id"]): str(row["density_stratum"])
        for row in predecessor["private"]["cases"]
    }
    core, guideline_sha = completeness_core_instructions()
    turns = []
    for episode in selected:
        segment_ids = [str(row["segment_id"]) for row in episode["segments"]]
        turn_name = stable_id(
            PHASE_ID,
            str(episode["episode_id"]),
            *segment_ids,
            prefix="v227_luna_gap_",
        )
        packets = []
        normalization_segments = []
        for segment in episode["segments"]:
            segment_id = str(segment["segment_id"])
            existing = existing_by_segment[segment_id]
            remaining = MAX_EVENTS_PER_SEGMENT - len(existing)
            if remaining < 0:
                raise JudgeV5SelectionV227Error(
                    "v227 predecessor exceeded final event cap"
                )
            packets.append(
                {
                    "segment_id": segment_id,
                    "segment_index": segment["segment_index"],
                    "remaining_event_slots": remaining,
                    "windows": segment["windows"],
                    "existing_events": [
                        judge.compact_empty_event_fields(event)
                        for event in existing
                    ],
                }
            )
            normalization_segments.append(
                {
                    "segment_id": segment_id,
                    "segment_text": segment["segment_text"],
                    "boundaries": segment["boundaries"],
                    "existing_events": existing,
                    "density_stratum": density_by_segment[segment_id],
                }
            )
        private_input = {
            "schema_version": V227_DESIGN_VERSION,
            "episode_id": episode["episode_id"],
            "prompt_packets": packets,
            "normalization_segments": normalization_segments,
            "privacy": "private_source_windows_context_and_events",
        }
        prompt = _gap_prompt(packets)
        base = app_eval.build_episode_base_instructions(
            core_instructions=core,
            episode=episode,
            episode_context=episode["episode_context"],
        )
        schema = app_eval.episode_batch_core_schema(
            episode_id=str(episode["episode_id"]),
            segment_ids=segment_ids,
            max_events_per_segment=MAX_EVENTS_PER_SEGMENT,
        )
        schema_bytes = len(_canonical_json(schema).encode("utf-8"))
        prompt_bytes = len(prompt.encode("utf-8"))
        if (
            prompt_bytes > MAXIMUM_PROMPT_BYTES_PER_TURN
            or schema_bytes > MAXIMUM_SCHEMA_BYTES_PER_TURN
        ):
            raise JudgeV5SelectionV227Error("v227 request exceeds frozen size cap")
        turns.append(
            {
                "turn_name": turn_name,
                "episode_id": str(episode["episode_id"]),
                "segment_ids": segment_ids,
                "private_input": private_input,
                "prompt": prompt,
                "base_instructions": base,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "base_instructions_bytes": len(base.encode("utf-8")),
                "schema_bytes": schema_bytes,
            }
        )
    turns.sort(key=lambda row: row["episode_id"])
    return {
        "schema_version": V227_DESIGN_VERSION,
        "guideline_sha256": guideline_sha,
        "turns": turns,
        "case_count": SEGMENT_COUNT,
        "episode_count": EPISODE_COUNT,
    }


def validate_gap_output(
    output: Any,
    *,
    schema: Mapping[str, Any],
    episode_id: str,
    segment_ids: Sequence[str],
) -> list[str]:
    if not isinstance(output, Mapping):
        return ["output_not_object"]
    try:
        _validate_schema(dict(schema), output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        return [f"schema:{type(exc).__name__}"]
    if output.get("episode_id") != episode_id:
        return ["episode_id"]
    rows = output.get("segments") or []
    if [row.get("segment_id") for row in rows] != list(segment_ids):
        return ["segment_order_or_coverage"]
    for row in rows:
        events = row.get("events") or []
        if (row.get("status") == "coded") != bool(events):
            return ["coded_status_event_presence"]
        if not events and row.get("status") != "no_signal":
            return ["empty_gap_output_must_be_no_signal"]
    return []


def _event_identity(event: Mapping[str, Any]) -> str:
    return _canonical_json(
        {
            key: value
            for key, value in event.items()
            if key not in {"confidence", "window_id"}
        }
    )


def _normalize_gap_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    prepared = turn["private_input"]["normalization_segments"]
    normalized, diagnostics = app_eval.normalize_episode_batch_output(
        dict(output),
        episode_id=str(turn["episode_id"]),
        prepared_segments=prepared,
        max_events_per_segment=MAX_EVENTS_PER_SEGMENT,
    )
    prepared_by_id = {str(row["segment_id"]): row for row in prepared}
    diagnostics_by_id = {str(row["segment_id"]): row for row in diagnostics}
    for row in normalized["segments"]:
        segment_id = str(row["segment_id"])
        source = prepared_by_id[segment_id]
        existing = source["existing_events"]
        existing_identities = {_event_identity(event) for event in existing}
        retained = [
            event
            for event in row.get("events") or []
            if _event_identity(event) not in existing_identities
        ]
        removed = len(row.get("events") or []) - len(retained)
        row["events"] = retained
        if retained:
            row["status"] = "coded"
            row["no_signal_reason"] = ""
        else:
            row["status"] = "no_signal"
            row["no_signal_reason"] = (
                "The independent LLM pass returned no additional grounded event."
            )
        combined_count = len(existing) + len(retained)
        diagnostic = diagnostics_by_id[segment_id]
        diagnostic["exact_existing_duplicates_removed"] = removed
        diagnostic["new_event_count"] = len(retained)
        diagnostic["existing_event_count"] = len(existing)
        diagnostic["combined_event_count"] = combined_count
        diagnostic["combined_event_cap_hit"] = (
            combined_count == MAX_EVENTS_PER_SEGMENT
        )
        diagnostic["combined_event_cap_exceeded"] = (
            combined_count > MAX_EVENTS_PER_SEGMENT
        )
        diagnostic["density_stratum"] = source["density_stratum"]
    return normalized, diagnostics


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized-output.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
    }


def _freeze_turn(root: Path, turn: Mapping[str, Any]) -> dict[str, Path]:
    paths = _turn_paths(root, str(turn["turn_name"]))
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], turn["private_input"])
    _write_private_text(paths["prompt"], str(turn["prompt"]))
    _write_private_text(paths["base"], str(turn["base_instructions"]))
    _write_immutable(paths["schema"], turn["schema"])
    return paths


def _load_frozen_turn(root: Path, row: Mapping[str, Any]) -> dict[str, Any]:
    turn_name = str(row["turn_name"])
    paths = _turn_paths(root, turn_name)
    for key in ("input", "prompt", "base", "schema"):
        if not _verify_record(row[key]):
            raise JudgeV5SelectionV227Error(
                f"v227 frozen request drifted: {turn_name}:{key}"
            )
    private_input = _load_json(paths["input"], f"v227 {turn_name} input")
    return {
        "turn_name": turn_name,
        "episode_id": str(private_input["episode_id"]),
        "segment_ids": [
            str(item["segment_id"])
            for item in private_input["normalization_segments"]
        ],
        "private_input": private_input,
        "prompt": paths["prompt"].read_text(encoding="utf-8"),
        "base_instructions": paths["base"].read_text(encoding="utf-8"),
        "schema": _load_json(paths["schema"], f"v227 {turn_name} schema"),
        "paths": paths,
    }


def _build_capacity_policy(
    root: Path,
    predecessor: Mapping[str, Any],
    turn_names: Sequence[str],
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAXIMUM_PHASE_TOTAL_TOKENS
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
    )
    audit = {
        "schema_version": V227_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v226_terminal": predecessor["records"]["v226_terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor[
                "terminal"
            ]["cumulative_known_usage_lower_bound"]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": MAXIMUM_PHASE_TOTAL_TOKENS,
            "promotion_total_token_bound": MAXIMUM_PROMOTION_TOKENS,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": (
                MINIMUM_REMAINING_RESERVE_PERCENT
            ),
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V227_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(turn_names),
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": MAXIMUM_PHASE_TOTAL_TOKENS,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(app_eval.__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(efficient_backtest.__file__).resolve(),
                Path(judge.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(v220.__file__).resolve(),
                Path(v222.__file__).resolve(),
                Path(v226.__file__).resolve(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    predecessor = _validate_predecessor()
    lock = _load_json(path, "v227 runtime lock")
    expected_runtime = {str(item) for item in _expected_runtime_paths()}
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    if (
        lock.get("schema_version") != V227_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or actual_runtime != expected_runtime
        or lock.get("semantic_turn_count") != EPISODE_COUNT
        or lock.get("model") != MODEL
        or lock.get("reasoning_effort") != EFFORT
        or lock.get("retry_count_per_turn") != 0
        or lock.get("predecessor") != list(predecessor["records"].values())
        or lock.get("extraction_replay_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV227Error("v227 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("predecessor") or []),
        lock.get("spec"),
        lock.get("design"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_requests") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV227Error("v227 runtime lock record drifted")
    reserve.load_reserve_capacity_policy(
        Path(lock["capacity_policy"]["path"])
    )
    return lock


def freeze_v227(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Optional[Path] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {
            "root": root,
            "terminal": _load_json(terminal_path, "v227 terminal"),
        }
    if any(root.iterdir()):
        spec_path = root / "attempt-spec.json"
        lock_path = root / "runtime-lock.json"
        if not spec_path.exists() or not lock_path.exists():
            raise JudgeV5SelectionV227Error(
                "v227 root is nonempty without a complete presemantic freeze"
            )
        if (root / "launch-receipt.json").exists():
            raise JudgeV5SelectionV227Error(
                "v227 launch exists without terminal; replay is prohibited"
            )
        verify_runtime_lock(lock_path)
        spec = _load_json(spec_path, "v227 spec")
        return {
            "root": root,
            "predecessor": _validate_predecessor(),
            "spec": spec,
            "spec_path": spec_path,
            "runtime_lock": lock_path,
            "capacity_policy": root / "capacity-policy.json",
            "turns": [
                _load_frozen_turn(root, row)
                for row in spec["frozen_turns"]
            ],
        }
    predecessor = _validate_predecessor()
    bundle = _load_prepared_turns(
        predecessor,
        database_path=database_path,
    )
    turn_records = []
    frozen_turns = []
    for turn in bundle["turns"]:
        paths = _freeze_turn(root, turn)
        records = {
            key: _record(paths[key])
            for key in ("input", "prompt", "base", "schema")
        }
        turn_records.extend(records.values())
        frozen_turns.append(
            {
                "turn_name": turn["turn_name"],
                "episode_id": turn["episode_id"],
                "segment_ids": turn["segment_ids"],
                **records,
            }
        )
    turn_names = [str(row["turn_name"]) for row in frozen_turns]
    capacity_paths = _build_capacity_policy(root, predecessor, turn_names)
    design = {
        "schema_version": V227_DESIGN_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "luna_low_independent_omission_only_over_frozen_sol_drafts",
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "episode_count": EPISODE_COUNT,
        "segment_count": SEGMENT_COUNT,
        "max_events_per_segment": MAX_EVENTS_PER_SEGMENT,
        "existing_extraction_outputs_reused_only": True,
        "not_an_extraction_replay": True,
        "source_windows_read_by_llm": True,
        "existing_events_read_by_llm": True,
        "reference_or_density_in_model_input": False,
        "semantic_regex_or_keyword_pruning": False,
        "prompt_or_rubric_tuned_on_holdout": False,
        "support_judge_authorized_before_structural_and_cost_gate": False,
        "production_amortized_base_tokens": BASE_CANDIDATE_PROJECTED_TOKENS,
        "maximum_promotion_tokens": MAXIMUM_PROMOTION_TOKENS,
        "production_amortized_token_target": 0.28,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "guideline_sha256": bundle["guideline_sha256"],
        "predecessor": predecessor["records"],
        "privacy": "private_source_and_events_sanitized_reports_only",
    }
    design_path = root / "completeness-design.json"
    _write_stable_time(design_path, design, "created_at")
    spec = {
        "schema_version": V227_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_semantic_attempt",
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": TIMEOUT_SECONDS,
        "declared_turn_count": EPISODE_COUNT,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "maximum_phase_total_tokens": MAXIMUM_PHASE_TOTAL_TOKENS,
        "maximum_promotion_tokens": MAXIMUM_PROMOTION_TOKENS,
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "api_key_billing_allowed": False,
        "raw_session_token_access_allowed": False,
        "codex_exec_semantic_calls_allowed": False,
        "semantic_regex_or_keyword_pruning_allowed": False,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "frozen_turns": frozen_turns,
        "predecessor": predecessor["records"],
        "design": _record(design_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "privacy": "private_source_and_events_sanitized_reports_only",
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {
        "schema_version": V227_RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _expected_runtime_paths()],
        "predecessor": list(predecessor["records"].values()),
        "spec": _record(spec_path),
        "design": _record(design_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_requests": turn_records,
        "semantic_turn_count": EPISODE_COUNT,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "retry_count_per_turn": 0,
        "extraction_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    lock_path = root / "runtime-lock.json"
    _write_stable_time(lock_path, lock, "created_at")
    verify_runtime_lock(lock_path)
    return {
        "root": root,
        "predecessor": predecessor,
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": lock_path,
        "capacity_policy": capacity_paths["policy"],
        "turns": [
            _load_frozen_turn(root, row) for row in spec["frozen_turns"]
        ],
    }


def _sidecar_accounting(
    root: Path, turn_names: Sequence[str]
) -> dict[str, Any]:
    usage = {field: 0 for field in USAGE_FIELDS}
    attempted = measured = unknown = 0
    sidecars = []
    for turn_name in turn_names:
        paths = _turn_paths(root, turn_name)
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        attempted += 1
        if not paths["sidecar"].exists():
            unknown += 1
            continue
        sidecar = _load_json(paths["sidecar"], f"v227 {turn_name} sidecar")
        sidecars.append(_record(paths["sidecar"]))
        try:
            turn_usage = _validate_usage(sidecar)
            if (
                sidecar.get("usage_status") != "measured"
                or sidecar.get("usage_complete") is not True
            ):
                raise JudgeV5SelectionV227Error("v227 usage is incomplete")
        except Exception:
            unknown += 1
            continue
        measured += 1
        usage = _sum_usage(usage, turn_usage)
    return {
        "attempted_turn_count": attempted,
        "measured_turn_count": measured,
        "unknown_usage_turn_count": unknown,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "sidecars": sidecars,
    }


def _validate_measured_turn(root: Path, turn_name: str) -> dict[str, int]:
    paths = _turn_paths(root, turn_name)
    checkpoint = _load_json(paths["capacity"], f"v227 {turn_name} capacity")
    sidecar = _load_json(paths["sidecar"], f"v227 {turn_name} sidecar")
    usage = _validate_usage(sidecar)
    if (
        checkpoint.get("cleared_for_semantic_turn") is not True
        or checkpoint.get("managed_chatgpt_auth_verified") is not True
        or checkpoint.get("rate_limit_reached_type") is not None
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or not paths["output"].is_file()
    ):
        raise JudgeV5SelectionV227Error(
            f"v227 measured turn contract drifted: {turn_name}"
        )
    return usage


def _phase_gate(
    *,
    turns: Sequence[Mapping[str, Any]],
    diagnostics: Sequence[Mapping[str, Any]],
    accounting: Mapping[str, Any],
) -> dict[str, Any]:
    usage = accounting["usage"]
    scaled = math.ceil(
        int(usage["total_tokens"])
        * PRODUCTION_SEGMENT_SCOPE
        / OBSERVED_SEGMENT_SCOPE
    )
    projected = BASE_CANDIDATE_PROJECTED_TOKENS + scaled
    ratio = projected / BASELINE_END_TO_END_TOKENS
    dense = [row for row in diagnostics if row["density_stratum"] == "dense"]
    checks = {
        "all_declared_turns_completed": accounting["measured_turn_count"]
        == EPISODE_COUNT,
        "usage_complete": accounting["accounting_complete"] is True,
        "phase_total_tokens_lte_120000": int(usage["total_tokens"])
        <= MAXIMUM_PHASE_TOTAL_TOKENS,
        "promotion_total_tokens_lte_77250": int(usage["total_tokens"])
        <= MAXIMUM_PROMOTION_TOKENS,
        "schema_status_success": len(diagnostics) == SEGMENT_COUNT
        and all(bool(row["status_ok"]) for row in diagnostics),
        "exact_evidence_rate_1": all(
            int(row["exactness_pruned_events"]) == 0 for row in diagnostics
        ),
        "metric_grounding_error_events_0": all(
            int(row["metric_grounding_error_events"]) == 0
            for row in diagnostics
        ),
        "combined_event_cap_exceeded_0": all(
            row["combined_event_cap_exceeded"] is False
            for row in diagnostics
        ),
        "new_grounded_event_count_gt_0": sum(
            int(row["new_event_count"]) for row in diagnostics
        )
        > 0,
        "each_dense_case_adds_grounded_event": len(dense) == 2
        and all(int(row["new_event_count"]) > 0 for row in dense),
        "production_amortized_total_token_ratio_lte_0_28": projected * 25
        <= BASELINE_END_TO_END_TOKENS * 7,
    }
    return {
        "schema_version": V227_GATE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(
            name for name, passed in checks.items() if not passed
        ),
        "completed_turn_count": accounting["measured_turn_count"],
        "not_started_turn_count": EPISODE_COUNT
        - accounting["attempted_turn_count"],
        "validated_segment_count": len(diagnostics),
        "new_event_count": sum(
            int(row["new_event_count"]) for row in diagnostics
        ),
        "dense_new_event_counts": [
            int(row["new_event_count"]) for row in dense
        ],
        "exact_existing_duplicates_removed": sum(
            int(row["exact_existing_duplicates_removed"])
            for row in diagnostics
        ),
        "usage": dict(usage),
        "repair_tokens_scaled_to_60_segments": scaled,
        "production_amortized_total_tokens": projected,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "support_audit_authorized": all(checks.values()),
        "alignment_audit_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _write_failure(
    *, root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    turn_names = [str(row["turn_name"]) for row in frozen["turns"]]
    accounting = _sidecar_accounting(root, turn_names)
    message = str(exc).encode("utf-8", errors="replace")
    failure = {
        "schema_version": V227_FAILURE_VERSION,
        "failed_at": now_iso(),
        "error_class": type(exc).__name__,
        "error_message_sha256": sha256_text(
            message.decode("utf-8", errors="replace")
        ),
        "error_message_bytes": len(message),
        **accounting,
        "semantic_retry_allowed": False,
        "support_audit_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "error_class_hash_length_and_aggregate_usage_only",
    }
    failure_path = root / "failure.json"
    _write_stable_time(failure_path, failure, "failed_at")
    source = frozen["predecessor"]["terminal"]
    known = _sum_usage(
        source["cumulative_known_usage_lower_bound"], accounting["usage"]
    )
    unknown = int(source["cumulative_unknown_usage_turn_count"]) + int(
        accounting["unknown_usage_turn_count"]
    )
    upper = int(source["cumulative_conservative_unknown_usage_upper_bound"]) + (
        int(accounting["unknown_usage_turn_count"])
        * MAXIMUM_TOTAL_TOKENS_PER_TURN
    )
    terminal = {
        "schema_version": V227_TERMINAL_VERSION,
        "state": "failed",
        "terminal_at": now_iso(),
        "terminal_reason": "infrastructure_or_semantic_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "support_audit_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "semantic_retry_allowed": False,
        "cumulative_known_usage_lower_bound": known,
        "cumulative_unknown_usage_turn_count": unknown,
        "cumulative_conservative_unknown_usage_upper_bound": upper,
        "failure": _record(failure_path),
        "spec": _record(frozen["spec_path"]),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "launch_receipt": _record(root / "launch-receipt.json"),
        **{key: value for key, value in accounting.items() if key != "sidecars"},
        "sidecars": accounting["sidecars"],
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[
            str(PINNED_CODEX_0_144_1),
            "app-server",
            "--stdio",
            "--strict-config",
        ]
    )


def _default_client_factory(
    policy_path: Path,
) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path,
        inner_factory=_inner_factory,
    )


async def run_v227(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Optional[Path] = None,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _default_client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v227 terminal")
    frozen = freeze_v227(
        output_dir=root,
        database_path=database_path,
    )
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise JudgeV5SelectionV227Error(
            "v227 launch receipt exists; semantic replay is prohibited"
        )
    launch = {
        "schema_version": V227_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "launched_at": now_iso(),
        "declared_turn_count": EPISODE_COUNT,
        "retry_count_per_turn": 0,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "managed_chatgpt_auth_only": True,
        "semantic_thread_or_turn_started_before_receipt": False,
        "existing_extraction_replayed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_immutable(launch_path, launch)
    started = time.monotonic()
    diagnostics: list[dict[str, Any]] = []
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                paths = turn["paths"]
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=turn["base_instructions"],
                    prompt=turn["prompt"],
                    output_schema=turn["schema"],
                    cwd=v220.PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=SEGMENTS_PER_EPISODE,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(
                    result.output, Mapping
                ):
                    raise JudgeV5SelectionV227Error(
                        f"v227 turn did not complete: {turn['turn_name']}"
                    )
                errors = validate_gap_output(
                    result.output,
                    schema=turn["schema"],
                    episode_id=turn["episode_id"],
                    segment_ids=turn["segment_ids"],
                )
                if errors:
                    raise JudgeV5SelectionV227Error(
                        f"v227 structured output validation failed: {len(errors)}"
                    )
                turn_usage = _validate_measured_turn(
                    root, str(turn["turn_name"])
                )
                normalized, turn_diagnostics = _normalize_gap_output(
                    result.output, turn
                )
                _write_immutable(paths["normalized"], normalized)
                _write_immutable(
                    paths["diagnostics"],
                    {"segments": turn_diagnostics},
                )
                diagnostics.extend(turn_diagnostics)
                dense_rows = [
                    row
                    for row in turn_diagnostics
                    if row["density_stratum"] == "dense"
                ]
                turn_ok = (
                    int(turn_usage["total_tokens"])
                    <= MAXIMUM_TOTAL_TOKENS_PER_TURN
                    and all(bool(row["status_ok"]) for row in turn_diagnostics)
                    and all(
                        int(row["exactness_pruned_events"]) == 0
                        for row in turn_diagnostics
                    )
                    and all(
                        row["combined_event_cap_exceeded"] is False
                        for row in turn_diagnostics
                    )
                    and len(dense_rows) == 1
                    and int(dense_rows[0]["new_event_count"]) > 0
                )
                if not turn_ok:
                    break
        turn_names = [str(row["turn_name"]) for row in frozen["turns"]]
        accounting = _sidecar_accounting(root, turn_names)
        if accounting["accounting_complete"] is not True:
            raise JudgeV5SelectionV227Error("v227 accounting is incomplete")
        gate = _phase_gate(
            turns=frozen["turns"],
            diagnostics=diagnostics,
            accounting=accounting,
        )
        gate_path = root / "completeness-gate.json"
        _write_immutable(gate_path, gate)
        source = frozen["predecessor"]["terminal"]
        cumulative = _sum_usage(
            source["cumulative_known_usage_lower_bound"], accounting["usage"]
        )
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": V227_TERMINAL_VERSION,
            "state": "completed" if passed else "development_strategy_not_accepted",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v227_luna_completeness_passed_support_audit_authorized"
                if passed
                else "v227_luna_completeness_structural_or_cost_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "support_audit_authorized": passed,
            "alignment_audit_authorized": False,
            "development_quality_passed": False,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "semantic_retry_allowed": False,
            "usage_status": accounting["usage_status"],
            "accounting_complete": accounting["accounting_complete"],
            "usage": accounting["usage"],
            "attempted_turn_count": accounting["attempted_turn_count"],
            "measured_turn_count": accounting["measured_turn_count"],
            "unknown_usage_turn_count": accounting["unknown_usage_turn_count"],
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": source[
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": source[
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "wall_seconds": round(time.monotonic() - started, 6),
            "gate": _record(gate_path),
            "sidecars": accounting["sidecars"],
            "spec": _record(frozen["spec_path"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "launch_receipt": _record(launch_path),
            "required_next_artifact_path": str(
                root.parent
                / (
                    "development-selection-v5_4-v228-luna-completeness-support"
                    if passed
                    else (
                        "development-selection-v5_4-v228-luna-"
                        "completeness-nonacceptance"
                    )
                )
                / "terminal.json"
            ),
        }
        _write_stable_time(terminal_path, terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if terminal_path.exists():
            return _load_json(terminal_path, "v227 terminal")
        return _write_failure(root=root, frozen=frozen, exc=exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run v227 Luna completeness diagnostic"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--database-path")
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v227(
            output_dir=Path(args.output_dir),
            database_path=(
                Path(args.database_path) if args.database_path else None
            ),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "support_audit_authorized": terminal.get(
                    "support_audit_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
                "holdout_authorized": terminal.get("holdout_authorized", False),
                "production_mutated": terminal.get("production_mutated", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
