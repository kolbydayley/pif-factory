from __future__ import annotations

"""Run the frozen v174 support pass over the observed v221 residual pool."""

import argparse
import asyncio
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5 as judge
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v174_exact_evidence_continuation as v174
from . import app_server_judge_v5_selection_v175_support as v175
from . import app_server_judge_v5_selection_v220_integrated_base_design as v220
from . import app_server_judge_v5_selection_v221_integrated_base_canary as v221
from . import (
    app_server_judge_v5_selection_v222_integrated_partial_nonacceptance as v222,
)
from . import codex_app_server
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
from .util import now_iso, sha256_text


V223_POOL_VERSION = "pif_app_server_judge_v5_4_selection_v223_pool_v1"
V223_DESIGN_VERSION = "pif_app_server_judge_v5_4_selection_v223_design_v1"
V223_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v223_spec_v1"
V223_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V223_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V223_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v223_runtime_lock_v1"
)
V223_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v223_launch_v1"
V223_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v223_audit_v1"
V223_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v223_failure_v1"
V223_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v223_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v223_frozen_support_audit"
TURN_NAME = "v223_frozen_support_residual_pool"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAXIMUM_TOTAL_TOKENS_PER_TURN = 70_000
MAXIMUM_PROMPT_BYTES = 120_000
MAXIMUM_SCHEMA_BYTES = 50_000
MINIMUM_REMAINING_RESERVE_PERCENT = 20
TIMEOUT_SECONDS = 1200.0
EXPECTED_CASE_COUNT = 3
EXPECTED_WITNESS_COUNT = 79
EXPECTED_REFERENCE_WITNESS_COUNT = 50
EXPECTED_CANDIDATE_WITNESS_COUNT = 29
DEFAULT_OUTPUT_ROOT = (
    v220.PIPELINE_ROOT
    / "development-selection-v5_4-v223-frozen-judge-residual-support"
).resolve()


class JudgeV5SelectionV223Error(RuntimeError):
    """The frozen support audit cannot be prepared, run, or adopted safely."""


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
            raise JudgeV5SelectionV223Error(f"frozen {path.name} drifted")
        return
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _v222_paths() -> dict[str, Path]:
    root = v222.DEFAULT_OUTPUT_ROOT
    return {
        "terminal": root / "terminal.json",
        "runtime_lock": root / "runtime-lock.json",
        "spec": root / "nonacceptance-spec.json",
        "gate": root / "partial-structural-gate.json",
        "private": root / "partial-adoption.private.json",
        "report": root / "development-nonacceptance-report.json",
        "next": root / "next-experiment.json",
    }


def _validate_v222_authorization() -> dict[str, Any]:
    paths = _v222_paths()
    root = v222.DEFAULT_OUTPUT_ROOT
    actual = {
        path.resolve() for path in root.rglob("*") if path.is_file()
    }
    expected = {path.resolve() for path in paths.values()}
    if actual != expected:
        raise JudgeV5SelectionV223Error("v222 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV223Error("v222 immutable artifact drifted")
    v222.verify_runtime_lock(paths["runtime_lock"])
    values = {
        name: _load_json(path, f"v222 {name}")
        for name, path in paths.items()
    }
    terminal = values["terminal"]
    gate = values["gate"]
    next_experiment = values["next"]
    if (
        terminal.get("state") != "development_strategy_not_accepted"
        or terminal.get("terminal_reason")
        != "v222_integrated_completeness_routing_gate_not_passed_"
        "remaining_turns_cancelled"
        or terminal.get("fresh_frozen_judge_audit_authorized") is not True
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("cumulative_known_usage_lower_bound")
        != v222.EXPECTED_CUMULATIVE_KNOWN_USAGE
        or gate.get("unflagged_dense_coverage_shortfall_count") != 2
        or gate.get("no_signal_candidate_positive_segment_count") != 1
        or gate.get(
            "remaining_turns_can_change_frozen_v220_strategy_verdict"
        )
        is not False
        or next_experiment.get("state") != "authorized_not_started"
        or next_experiment.get("strategy")
        != "frozen_v174_side_free_observed_residual_audit"
        or next_experiment.get("semantic_scope", {}).get(
            "judge_protocol_changed"
        )
        is not False
    ):
        raise JudgeV5SelectionV223Error("v222 authorization contract drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        **values,
    }


def reconstruct_segment_text(windows: Sequence[Mapping[str, Any]]) -> str:
    if not windows:
        raise JudgeV5SelectionV223Error("segment packet has no windows")
    length = max(int(window["extract_end"]) for window in windows)
    characters: list[Optional[str]] = [None] * length
    for window in windows:
        start = int(window["extract_start"])
        end = int(window["extract_end"])
        text = str(window["extract_text"])
        if end - start != len(text):
            raise JudgeV5SelectionV223Error("window length contract drifted")
        for offset, character in enumerate(text, start=start):
            existing = characters[offset]
            if existing is not None and existing != character:
                raise JudgeV5SelectionV223Error("overlapping windows conflict")
            characters[offset] = character
    if any(character is None for character in characters):
        raise JudgeV5SelectionV223Error("window packet does not cover source")
    return "".join(character or "" for character in characters)


def _source_texts_from_v221_prompts(
    private: Mapping[str, Any],
) -> dict[str, str]:
    result: dict[str, str] = {}
    for turn in private.get("turns") or []:
        turn_name = str(turn["turn_name"])
        paths = v221._turn_artifact_paths(v221.DEFAULT_OUTPUT_ROOT, turn_name)
        prompt = paths["prompt"].read_text(encoding="utf-8")
        prefix = "# Segment-specific IDs and fixed evidence windows\n"
        if not prompt.startswith(prefix):
            raise JudgeV5SelectionV223Error("v221 prompt envelope drifted")
        packet = json.loads(prompt[len(prefix) :])
        for segment in packet:
            segment_id = str(segment["segment_id"])
            if segment_id in result:
                raise JudgeV5SelectionV223Error("duplicate v221 source packet")
            result[segment_id] = reconstruct_segment_text(segment["windows"])
    return result


def _event_claim(event: Mapping[str, Any]) -> str:
    claim = event.get("claim_text")
    if not isinstance(claim, str) or not claim.strip():
        raise JudgeV5SelectionV223Error("residual event has no claim text")
    return claim


def build_residual_pool(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    private = predecessor["private"]
    source_texts = _source_texts_from_v221_prompts(private)
    v220_reference = _load_json(
        v220.DEFAULT_OUTPUT_ROOT / "shared-reference-seed-v1.json",
        "v220 shared reference",
    )
    reference_by_segment = {
        str(row["segment_id"]): row for row in v220_reference["references"]
    }
    manifest_value = _load_json(
        v220.DEFAULT_OUTPUT_ROOT / "manifest-v4.json", "v220 manifest"
    )
    metadata = {
        str(segment["segment_id"]): segment
        for episode in manifest_value["episodes"]
        for segment in episode["segments"]
    }
    candidate_by_segment = {
        str(segment["segment_id"]): segment
        for turn in private["turns"]
        for segment in turn["normalized"]["segments"]
    }
    private_cases = private.get("cases") or []
    dense_ids = sorted(
        str(row["segment_id"])
        for row in private_cases
        if row["density_stratum"] == "dense"
    )
    no_signal_positive_ids = sorted(
        str(row["segment_id"])
        for row in private_cases
        if row["density_stratum"] == "no_signal"
        and int(row["output_events"]) > 0
    )
    selected_ids = dense_ids + no_signal_positive_ids
    if len(dense_ids) != 2 or len(no_signal_positive_ids) != 1:
        raise JudgeV5SelectionV223Error("v223 residual case selection drifted")
    if any(
        segment_id not in source_texts
        or segment_id not in reference_by_segment
        or segment_id not in candidate_by_segment
        or segment_id not in metadata
        for segment_id in selected_ids
    ):
        raise JudgeV5SelectionV223Error("v223 residual source coverage drifted")

    cases = []
    units = []
    origin_rows = []
    for segment_id in selected_ids:
        source_excerpt = source_texts[segment_id]
        if sha256_text(source_excerpt) != metadata[segment_id]["text_sha256"]:
            raise JudgeV5SelectionV223Error("reconstructed source hash drifted")
        density = str(metadata[segment_id]["density_stratum"])
        case_id = "case_" + sha256_text(f"v223|{segment_id}")[:24]
        reference_events = list(
            reference_by_segment[segment_id]["golden_output"].get(
                "discourse_events"
            )
            or []
        )
        candidate_events = list(
            candidate_by_segment[segment_id].get("events") or []
        )
        witnesses = []
        for origin, events in (
            ("reference", reference_events),
            ("candidate", candidate_events),
        ):
            for event_index, event in enumerate(events):
                event_hash = sha256_text(_canonical_json(event))
                witness_id = "w_" + sha256_text(
                    f"v223|{case_id}|{origin}|{event_index}|{event_hash}"
                )[:24]
                witness = {
                    "witness_id": witness_id,
                    "event": dict(event),
                }
                witnesses.append(witness)
                units.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "proposition": {"claim_text": _event_claim(event)},
                        "source_excerpt": source_excerpt,
                    }
                )
                origin_rows.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "segment_id": segment_id,
                        "density_stratum": density,
                        "origin": origin,
                        "event_index": event_index,
                        "event_sha256": event_hash,
                    }
                )
        witnesses.sort(key=lambda row: str(row["witness_id"]))
        cases.append(
            {
                "case_id": case_id,
                "segment_id": segment_id,
                "density_stratum": density,
                "source_excerpt": source_excerpt,
                "witnesses": witnesses,
                "reference_event_count": len(reference_events),
                "candidate_event_count": len(candidate_events),
            }
        )
    cases.sort(key=lambda row: str(row["case_id"]))
    units.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    origin_rows.sort(
        key=lambda row: (str(row["case_id"]), str(row["witness_id"]))
    )
    counts = Counter(str(row["origin"]) for row in origin_rows)
    if (
        len(cases) != EXPECTED_CASE_COUNT
        or len(units) != EXPECTED_WITNESS_COUNT
        or counts
        != Counter(
            {
                "reference": EXPECTED_REFERENCE_WITNESS_COUNT,
                "candidate": EXPECTED_CANDIDATE_WITNESS_COUNT,
            }
        )
    ):
        raise JudgeV5SelectionV223Error("v223 residual pool coverage drifted")
    support_value = v143._support_input(units)
    prompt = v175.compact_support_prompt(units)
    schema = v143.support_output_schema(support_value)
    schema_text = _canonical_json(schema)
    if (
        len(prompt.encode("utf-8")) > MAXIMUM_PROMPT_BYTES
        or len(schema_text.encode("utf-8")) > MAXIMUM_SCHEMA_BYTES
    ):
        raise JudgeV5SelectionV223Error("v223 request exceeds frozen size cap")
    return {
        "schema_version": V223_POOL_VERSION,
        "cases": cases,
        "units": units,
        "support_value": support_value,
        "origin_rows": origin_rows,
        "prompt": prompt,
        "schema": schema,
        "prompt_sha256": sha256_text(prompt),
        "prompt_bytes": len(prompt.encode("utf-8")),
        "schema_sha256": sha256_text(schema_text),
        "schema_bytes": len(schema_text.encode("utf-8")),
    }


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAXIMUM_TOTAL_TOKENS_PER_TURN
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
    )
    audit = {
        "schema_version": V223_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v222_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor[
                "terminal"
            ]["cumulative_known_usage_lower_bound"]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": predecessor["terminal"][
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": (
                MINIMUM_REMAINING_RESERVE_PERCENT
            ),
        },
    }
    _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V223_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": (
            MINIMUM_REMAINING_RESERVE_PERCENT
        ),
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_immutable(policy_path, policy)
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v222._expected_runtime_paths())
            | {
                Path(__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(judge.__file__).resolve(),
                Path(v143.__file__).resolve(),
                Path(v174.__file__).resolve(),
                Path(v175.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v223 runtime lock")
    expected_runtime = {str(item) for item in _expected_runtime_paths()}
    actual_runtime = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping) and isinstance(record.get("path"), str)
    }
    if (
        lock.get("schema_version") != V223_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or actual_runtime != expected_runtime
        or lock.get("semantic_turn_count") != 1
        or lock.get("retry_count_per_turn") != 0
        or lock.get("extraction_replay_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV223Error("v223 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v222_attempt") or []),
        *(lock.get("frozen_judge") or []),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_requests") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV223Error("v223 runtime lock record drifted")
    reserve.load_reserve_capacity_policy(
        Path(lock["capacity_policy"]["path"])
    )
    return lock


def freeze_v223(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {
            "root": root,
            "terminal": _load_json(terminal_path, "v223 terminal"),
        }
    if any(root.iterdir()):
        spec_path = root / "attempt-spec.json"
        lock_path = root / "runtime-lock.json"
        if not spec_path.exists() or not lock_path.exists():
            raise JudgeV5SelectionV223Error(
                "v223 root is nonempty without a complete presemantic freeze"
            )
        if (root / "launch-receipt.json").exists():
            raise JudgeV5SelectionV223Error(
                "v223 launch exists without terminal; replay is prohibited"
            )
        verify_runtime_lock(lock_path)
        return {
            "root": root,
            "predecessor": _validate_v222_authorization(),
            "spec": _load_json(spec_path, "v223 spec"),
            "spec_path": spec_path,
            "runtime_lock": lock_path,
            "capacity_policy": root / "capacity-policy.json",
            "pool": _load_json(root / "residual-pool.private.json", "v223 pool"),
            "support_value": _load_json(
                root / "support-input.private.json", "v223 support input"
            ),
            "prompt": (root / "support-prompt.private.md").read_text(
                encoding="utf-8"
            ),
            "schema": _load_json(root / "support-schema.json", "v223 schema"),
        }

    predecessor = _validate_v222_authorization()
    frozen_judge = v222._validate_frozen_v174_judge()
    protocol_record = frozen_judge["terminal"]["protocol"]
    protocol = _load_json(
        Path(protocol_record["path"]), "v174 development judge protocol"
    )
    if (
        protocol.get("support_instructions_sha256")
        != sha256_text(v143.support_base_instructions_v143())
        or protocol.get("support_model") != MODEL
        or protocol.get("reasoning_effort") != EFFORT
    ):
        raise JudgeV5SelectionV223Error("v174 support protocol drifted")
    pool = build_residual_pool(predecessor)
    design = {
        "schema_version": V223_DESIGN_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "frozen_v174_pointwise_support_over_observed_residuals",
        "case_count": EXPECTED_CASE_COUNT,
        "witness_count": EXPECTED_WITNESS_COUNT,
        "reference_witness_count": EXPECTED_REFERENCE_WITNESS_COUNT,
        "candidate_witness_count": EXPECTED_CANDIDATE_WITNESS_COUNT,
        "semantic_pruning_performed": False,
        "semantic_matching_performed": False,
        "side_labels_in_model_input": False,
        "source_serialization": "v175_measured_compact_case_shared_source",
        "support_instruction_hash": sha256_text(
            v143.support_base_instructions_v143()
        ),
        "support_model": MODEL,
        "reasoning_effort": EFFORT,
        "semantic_turn_count": 1,
        "retry_count_per_turn": 0,
        "prompt_bytes": pool["prompt_bytes"],
        "schema_bytes": pool["schema_bytes"],
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "privacy": "opaque_ids_counts_hashes_and_sizes_only",
    }
    design_path = root / "support-design.json"
    pool_path = root / "residual-pool.private.json"
    input_path = root / "support-input.private.json"
    origin_path = root / "origin-map.private.json"
    instructions_path = root / "support-instructions.private.md"
    prompt_path = root / "support-prompt.private.md"
    schema_path = root / "support-schema.json"
    _write_immutable(design_path, design)
    _write_immutable(
        pool_path,
        {
            "schema_version": V223_POOL_VERSION,
            "cases": pool["cases"],
            "privacy": "private_source_events_and_ids",
        },
    )
    _write_immutable(input_path, pool["support_value"])
    _write_immutable(
        origin_path,
        {
            "schema_version": V223_POOL_VERSION,
            "rows": pool["origin_rows"],
            "privacy": "private_origin_map_no_source_or_event_text",
        },
    )
    _write_private_text(
        instructions_path, v143.support_base_instructions_v143()
    )
    _write_private_text(prompt_path, pool["prompt"])
    _write_immutable(schema_path, pool["schema"])
    capacity_paths = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V223_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_semantic_attempt",
        "turn_plan": [TURN_NAME],
        "declared_turn_count": 1,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": TIMEOUT_SECONDS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "api_key_billing_allowed": False,
        "raw_session_token_access_allowed": False,
        "codex_exec_semantic_calls_allowed": False,
        "semantic_regex_or_keyword_pruning_allowed": False,
        "extraction_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "v222_attempt": predecessor["records"],
        "frozen_judge": frozen_judge["records"],
        "frozen_inputs": {
            "design": _record(design_path),
            "pool": _record(pool_path),
            "support_input": _record(input_path),
            "origin_map": _record(origin_path),
            "instructions": _record(instructions_path),
            "prompt": _record(prompt_path),
            "schema": _record(schema_path),
        },
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {
        "schema_version": V223_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _expected_runtime_paths()],
        "v222_attempt": list(predecessor["records"].values()),
        "frozen_judge": list(frozen_judge["records"].values()),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_requests": [
            _record(design_path),
            _record(pool_path),
            _record(input_path),
            _record(origin_path),
            _record(instructions_path),
            _record(prompt_path),
            _record(schema_path),
        ],
        "semantic_turn_count": 1,
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
        "frozen_judge": frozen_judge,
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": lock_path,
        "capacity_policy": capacity_paths["policy"],
        "pool": pool,
        "support_value": pool["support_value"],
        "prompt": pool["prompt"],
        "schema": pool["schema"],
    }


def _support_receipts(output: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    for row in output["units"]:
        rows.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "proposition_verdict": row["support_status"],
                "proposition_evidence_spans": row[
                    "source_evidence_spans"
                ],
                "structured_field_verdict": "abstain",
                "field_issue_fields": [],
                "field_evidence_spans": [],
            }
        )
    rows.sort(key=lambda row: (row["case_id"], row["witness_id"]))
    return {
        "schema_version": judge.POINTWISE_OUTPUT_VERSION,
        "units": rows,
        "side_free": True,
        "claim_support_and_field_correctness_separate": True,
    }


def score_support_output(
    *,
    output: Mapping[str, Any],
    support_value: Mapping[str, Any],
    origin_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    errors = v143.validate_support_output(output, support_value)
    if errors:
        raise JudgeV5SelectionV223Error(
            "v223 frozen support output validation failed"
        )
    by_id = {str(row["witness_id"]): row for row in output["units"]}
    origin_by_id = {
        str(row["witness_id"]): row for row in origin_rows
    }
    if set(by_id) != set(origin_by_id):
        raise JudgeV5SelectionV223Error("v223 support origin coverage drifted")
    rows = []
    for witness_id, decision in by_id.items():
        origin = origin_by_id[witness_id]
        rows.append(
            {
                **dict(origin),
                "support_status": decision["support_status"],
                "source_evidence_span_count": len(
                    decision["source_evidence_spans"]
                ),
            }
        )
    by_case: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_case.setdefault(str(row["case_id"]), []).append(row)
    case_summaries = []
    alignment_ready = True
    no_signal_disposition = "not_present"
    for case_id, case_rows in sorted(by_case.items()):
        density = str(case_rows[0]["density_stratum"])
        status_counts = Counter(
            (str(row["origin"]), str(row["support_status"]))
            for row in case_rows
        )
        supported_reference = status_counts[("reference", "supported")]
        supported_candidate = status_counts[("candidate", "supported")]
        if density == "dense" and (
            supported_reference < 1 or supported_candidate < 1
        ):
            alignment_ready = False
        if density == "no_signal":
            candidate_statuses = {
                str(row["support_status"])
                for row in case_rows
                if row["origin"] == "candidate"
            }
            if len(candidate_statuses) != 1:
                raise JudgeV5SelectionV223Error(
                    "v223 no-signal disposition is ambiguous"
                )
            no_signal_disposition = next(iter(candidate_statuses))
        case_summaries.append(
            {
                "case_id": case_id,
                "density_stratum": density,
                "reference_witness_count": sum(
                    row["origin"] == "reference" for row in case_rows
                ),
                "candidate_witness_count": sum(
                    row["origin"] == "candidate" for row in case_rows
                ),
                "supported_reference_witness_count": supported_reference,
                "supported_candidate_witness_count": supported_candidate,
                "unsupported_reference_witness_count": status_counts[
                    ("reference", "unsupported")
                ],
                "unsupported_candidate_witness_count": status_counts[
                    ("candidate", "unsupported")
                ],
                "abstain_reference_witness_count": status_counts[
                    ("reference", "abstain")
                ],
                "abstain_candidate_witness_count": status_counts[
                    ("candidate", "abstain")
                ],
            }
        )
    status_counts = Counter(
        str(row["support_status"]) for row in rows
    )
    audit = {
        "schema_version": V223_AUDIT_VERSION,
        "passed": alignment_ready,
        "witness_count": len(rows),
        "support_status_counts": dict(sorted(status_counts.items())),
        "case_summaries": case_summaries,
        "no_signal_candidate_event_support_status": no_signal_disposition,
        "exact_evidence_validated": True,
        "alignment_audit_authorized": alignment_ready,
        "semantic_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "sanitized_counts_and_statuses_no_source_or_event_text",
    }
    private = {
        "schema_version": V223_AUDIT_VERSION,
        "rows": rows,
        "privacy": "private_opaque_ids_origins_and_statuses_no_source_text",
    }
    return audit, private


def _sidecar_accounting(root: Path) -> dict[str, Any]:
    sidecar_path = _turn_paths(root)["sidecar"]
    capacity_path = _turn_paths(root)["capacity"]
    if not sidecar_path.exists() and not capacity_path.exists():
        return {
            "attempted_turn_count": 0,
            "measured_turn_count": 0,
            "unknown_usage_turn_count": 0,
            "usage_status": "not_started",
            "accounting_complete": False,
            "usage": {field: 0 for field in USAGE_FIELDS},
        }
    try:
        sidecar = _load_json(sidecar_path, "v223 sidecar")
        usage = _validate_usage(sidecar)
        if (
            sidecar.get("usage_complete") is not True
            or sidecar.get("usage_status") != "measured"
        ):
            raise JudgeV5SelectionV223Error("v223 sidecar usage incomplete")
    except Exception:
        return {
            "attempted_turn_count": 1,
            "measured_turn_count": 0,
            "unknown_usage_turn_count": 1,
            "usage_status": "unknown",
            "accounting_complete": False,
            "usage": {field: 0 for field in USAGE_FIELDS},
        }
    return {
        "attempted_turn_count": 1,
        "measured_turn_count": 1,
        "unknown_usage_turn_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": usage,
    }


def _validate_measured_turn(root: Path) -> dict[str, int]:
    paths = _turn_paths(root)
    capacity_value = _load_json(paths["capacity"], "v223 capacity")
    sidecar = _load_json(paths["sidecar"], "v223 sidecar")
    usage = _validate_usage(sidecar)
    if (
        capacity_value.get("cleared_for_semantic_turn") is not True
        or capacity_value.get("managed_chatgpt_auth_verified") is not True
        or capacity_value.get("rate_limit_reached_type") is not None
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
    ):
        raise JudgeV5SelectionV223Error("v223 measured turn contract drifted")
    return usage


def _write_failure(
    *, root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    accounting = _sidecar_accounting(root)
    message = str(exc).encode("utf-8", errors="replace")
    failure = {
        "schema_version": V223_FAILURE_VERSION,
        "failed_at": now_iso(),
        "error_class": type(exc).__name__,
        "error_message_sha256": sha256_text(
            message.decode("utf-8", errors="replace")
        ),
        "error_message_bytes": len(message),
        **accounting,
        "semantic_retry_allowed": False,
        "alignment_audit_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "error_class_hash_length_and_aggregate_usage_only",
    }
    failure_path = root / "failure.json"
    _write_stable_time(failure_path, failure, "failed_at")
    predecessor = frozen["predecessor"]["terminal"]
    known = dict(predecessor["cumulative_known_usage_lower_bound"])
    for field in USAGE_FIELDS:
        known[field] += int(accounting["usage"].get(field) or 0)
    unknown = int(predecessor["cumulative_unknown_usage_turn_count"]) + int(
        accounting["unknown_usage_turn_count"]
    )
    upper = int(
        predecessor["cumulative_conservative_unknown_usage_upper_bound"]
    ) + int(accounting["unknown_usage_turn_count"]) * int(
        MAXIMUM_TOTAL_TOKENS_PER_TURN
    )
    terminal = {
        "schema_version": V223_TERMINAL_VERSION,
        "state": "failed",
        "terminal_at": now_iso(),
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "development_winner_frozen": False,
        "alignment_audit_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "semantic_retry_allowed": False,
        "usage_status": accounting["usage_status"],
        "accounting_complete": accounting["accounting_complete"],
        "usage": accounting["usage"],
        "cumulative_known_usage_lower_bound": known,
        "cumulative_unknown_usage_turn_count": unknown,
        "cumulative_conservative_unknown_usage_upper_bound": upper,
        "failure": _record(failure_path),
        "spec": _record(frozen["spec_path"]),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "launch_receipt": _record(root / "launch-receipt.json"),
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


async def run_v223(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _default_client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v223 terminal")
    frozen = freeze_v223(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise JudgeV5SelectionV223Error(
            "v223 launch receipt exists; semantic replay is prohibited"
        )
    launch = {
        "schema_version": V223_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "launched_at": now_iso(),
        "declared_turn_count": 1,
        "retry_count_per_turn": 0,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "managed_chatgpt_auth_only": True,
        "semantic_thread_or_turn_started_before_receipt": False,
        "extraction_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_immutable(launch_path, launch)
    started = time.monotonic()
    paths = _turn_paths(root)
    paths["root"].mkdir(parents=True, exist_ok=True)
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=v143.support_base_instructions_v143(),
                prompt=frozen["prompt"],
                output_schema=frozen["schema"],
                cwd=v220.PROJECT_ROOT,
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=EXPECTED_WITNESS_COUNT,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise JudgeV5SelectionV223Error("v223 support turn did not complete")
        persisted = _load_json(paths["output"], "v223 support output")
        if _canonical_json(persisted) != _canonical_json(result.output):
            raise JudgeV5SelectionV223Error("v223 persisted output drifted")
        origin_map = _load_json(
            root / "origin-map.private.json", "v223 origin map"
        )
        audit, private_score = score_support_output(
            output=persisted,
            support_value=frozen["support_value"],
            origin_rows=origin_map["rows"],
        )
        receipts = _support_receipts(persisted)
        receipts_path = root / "frozen-support-receipts.private.json"
        private_score_path = root / "support-score.private.json"
        audit_path = root / "support-audit.json"
        _write_immutable(receipts_path, receipts)
        _write_immutable(private_score_path, private_score)
        _write_immutable(audit_path, audit)
        measured_usage = _validate_measured_turn(root)
        accounting = _sidecar_accounting(root)
        if (
            not accounting["accounting_complete"]
            or accounting["usage"] != measured_usage
        ):
            raise JudgeV5SelectionV223Error("v223 usage accounting incomplete")
        predecessor = frozen["predecessor"]["terminal"]
        known = dict(predecessor["cumulative_known_usage_lower_bound"])
        for field in USAGE_FIELDS:
            known[field] += int(accounting["usage"][field])
        terminal = {
            "schema_version": V223_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v223_frozen_support_audit_completed_alignment_authorized"
                if audit["alignment_audit_authorized"]
                else "v223_frozen_support_audit_completed_alignment_not_authorized"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "semantic_quality_passed": False,
            "development_winner_frozen": False,
            "alignment_audit_authorized": audit[
                "alignment_audit_authorized"
            ],
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "semantic_retry_allowed": False,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": accounting["usage"],
            "cumulative_known_usage_lower_bound": known,
            "cumulative_unknown_usage_turn_count": predecessor[
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": predecessor[
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "wall_seconds": round(time.monotonic() - started, 6),
            "support_audit": _record(audit_path),
            "support_receipts": _record(receipts_path),
            "support_score_private": _record(private_score_path),
            "output": _record(paths["output"]),
            "sidecar": _record(paths["sidecar"]),
            "capacity": _record(paths["capacity"]),
            "spec": _record(frozen["spec_path"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "launch_receipt": _record(launch_path),
            "required_next_artifact_path": str(
                root.parent
                / "development-selection-v5_4-v224-frozen-alignment-audit"
                / "terminal.json"
            ),
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except BaseException as exc:
        if terminal_path.exists():
            return _load_json(terminal_path, "v223 terminal")
        return _write_failure(root=root, frozen=frozen, exc=exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run v223 frozen residual support audit"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v223(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "alignment_audit_authorized": terminal[
                    "alignment_audit_authorized"
                ],
                "usage_status": terminal["usage_status"],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
