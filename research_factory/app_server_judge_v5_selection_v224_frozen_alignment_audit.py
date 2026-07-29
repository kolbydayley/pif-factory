from __future__ import annotations

"""Align the v223 support-positive residual pool in two frozen permutations."""

import argparse
import asyncio
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5 as judge
from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_selection_v220_integrated_base_design as v220
from . import (
    app_server_judge_v5_selection_v222_integrated_partial_nonacceptance as v222,
)
from . import app_server_judge_v5_selection_v223_frozen_support_audit as v223
from . import app_server_llm_judge
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
from .util import now_iso, sha256_text, stable_id


V224_POOL_VERSION = "pif_app_server_judge_v5_4_selection_v224_pool_v1"
V224_DESIGN_VERSION = "pif_app_server_judge_v5_4_selection_v224_design_v1"
V224_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v224_spec_v1"
V224_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V224_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V224_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v224_runtime_lock_v1"
)
V224_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v224_launch_v1"
V224_SCORE_VERSION = "pif_app_server_judge_v5_4_selection_v224_score_v1"
V224_WINNER_VERSION = "pif_app_server_judge_v5_4_selection_v224_winner_v1"
V224_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v224_failure_v1"
V224_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v224_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v224_frozen_alignment_audit"
TURN_NAMES = (
    "v224_frozen_alignment_base",
    "v224_frozen_alignment_balanced_canary",
)
PERMUTATIONS = ("base", "balanced_canary")
MODEL = "gpt-5.5"
EFFORT = "high"
MAXIMUM_TOTAL_TOKENS_PER_TURN = 180_000
MAXIMUM_PROMPT_BYTES = 180_000
MAXIMUM_SCHEMA_BYTES = 20_000
MINIMUM_REMAINING_RESERVE_PERCENT = 20
TIMEOUT_SECONDS = 1200.0
EXPECTED_CASE_COUNT = 3
EXPECTED_OBSERVED_SEGMENT_COUNT = 4
EXPECTED_WITNESS_COUNT = 79
EXPECTED_REFERENCE_WITNESS_COUNT = 50
EXPECTED_CANDIDATE_WITNESS_COUNT = 29
NONINFERIORITY_MARGIN = 0.03
TOKEN_RATIO_TARGET = 0.28
DEFAULT_OUTPUT_ROOT = (
    v220.PIPELINE_ROOT
    / "development-selection-v5_4-v224-frozen-alignment-audit"
).resolve()


class JudgeV5SelectionV224Error(RuntimeError):
    """The frozen alignment audit cannot be prepared, run, or adopted safely."""


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
            raise JudgeV5SelectionV224Error(f"frozen {path.name} drifted")
        return
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(
    left: Mapping[str, int], right: Mapping[str, int]
) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _v223_paths() -> dict[str, Path]:
    root = v223.DEFAULT_OUTPUT_ROOT
    turn_root = root / "turns" / v223.TURN_NAME.replace("_", "-")
    return {
        "spec": root / "attempt-spec.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "receipts": root / "frozen-support-receipts.private.json",
        "launch": root / "launch-receipt.json",
        "origin": root / "origin-map.private.json",
        "pool": root / "residual-pool.private.json",
        "runtime_lock": root / "runtime-lock.json",
        "audit": root / "support-audit.json",
        "design": root / "support-design.json",
        "support_input": root / "support-input.private.json",
        "instructions": root / "support-instructions.private.md",
        "prompt": root / "support-prompt.private.md",
        "schema": root / "support-schema.json",
        "private_score": root / "support-score.private.json",
        "terminal": root / "terminal.json",
        "turn_capacity": turn_root / "capacity.json",
        "turn_output": turn_root / "output.private.json",
        "turn_sidecar": turn_root / "sidecar.json",
    }


def _validate_v223_success() -> dict[str, Any]:
    root = v223.DEFAULT_OUTPUT_ROOT
    paths = _v223_paths()
    actual = {path.resolve() for path in root.rglob("*") if path.is_file()}
    expected = {path.resolve() for path in paths.values()}
    if actual != expected:
        raise JudgeV5SelectionV224Error("v223 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV224Error("v223 immutable artifact drifted")
    v223.verify_runtime_lock(paths["runtime_lock"])
    values = {
        name: _load_json(path, f"v223 {name}")
        for name, path in paths.items()
        if path.suffix == ".json"
    }
    terminal = values["terminal"]
    audit = values["audit"]
    receipts = values["receipts"]
    usage = v223._validate_measured_turn(root)
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v223_frozen_support_audit_completed_alignment_authorized"
        or terminal.get("alignment_audit_authorized") is not True
        or terminal.get("semantic_quality_passed") is not False
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != usage
        or audit.get("passed") is not True
        or audit.get("witness_count") != EXPECTED_WITNESS_COUNT
        or audit.get("support_status_counts") != {
            "supported": EXPECTED_WITNESS_COUNT
        }
        or audit.get("exact_evidence_validated") is not True
        or audit.get("no_signal_candidate_event_support_status")
        != "supported"
        or audit.get("alignment_audit_authorized") is not True
        or receipts.get("schema_version") != judge.POINTWISE_OUTPUT_VERSION
        or len(receipts.get("units") or []) != EXPECTED_WITNESS_COUNT
        or any(
            row.get("proposition_verdict") != "supported"
            for row in receipts.get("units") or []
        )
    ):
        raise JudgeV5SelectionV224Error("v223 alignment authorization drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "audit": audit,
        "receipts": receipts,
        "pool": values["pool"],
        "origin": values["origin"],
        "v222": v223._validate_v222_authorization(),
    }


def build_v224_pool(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    raw_pool = predecessor["pool"]
    origin_rows = predecessor["origin"].get("rows") or []
    origin_by_id = {str(row["witness_id"]): row for row in origin_rows}
    if len(origin_by_id) != EXPECTED_WITNESS_COUNT:
        raise JudgeV5SelectionV224Error("v223 origin coverage drifted")
    source_by_segment = {
        str(row["segment_id"]): row
        for row in predecessor["v222"]["private"].get("cases") or []
    }
    case_id_map = {
        str(case["case_id"]): stable_id(
            "v224", "case", str(case["case_id"]), prefix="jcase_"
        )
        for case in raw_pool.get("cases") or []
    }
    witness_id_map = {
        str(witness["witness_id"]): stable_id(
            "v224",
            "witness",
            str(witness["witness_id"]),
            prefix="wit_",
        )
        for case in raw_pool.get("cases") or []
        for witness in case.get("witnesses") or []
    }
    if (
        len(case_id_map) != EXPECTED_CASE_COUNT
        or len(witness_id_map) != EXPECTED_WITNESS_COUNT
    ):
        raise JudgeV5SelectionV224Error("v224 normalized ID coverage drifted")
    cases = []
    mapping_rows = []
    observed_segments = set()
    for case in raw_pool["cases"]:
        legacy_case_id = str(case["case_id"])
        segment_id = str(case["segment_id"])
        source_row = source_by_segment.get(segment_id)
        if source_row is None:
            raise JudgeV5SelectionV224Error("v224 source mapping is incomplete")
        observed_segments.add(segment_id)
        rendered = {"reference": [], "candidate": []}
        for witness in case["witnesses"]:
            legacy_witness_id = str(witness["witness_id"])
            origin = origin_by_id.get(legacy_witness_id)
            if origin is None or origin.get("origin") not in rendered:
                raise JudgeV5SelectionV224Error("v224 witness origin drifted")
            event_sha256 = sha256_text(_canonical_json(witness["event"]))
            if origin.get("event_sha256") != event_sha256:
                raise JudgeV5SelectionV224Error("v224 event provenance drifted")
            new_witness_id = witness_id_map[legacy_witness_id]
            rendered[str(origin["origin"])].append(
                {
                    "witness_id": new_witness_id,
                    "event": witness["event"],
                }
            )
            mapping_rows.append(
                {
                    "case_id": case_id_map[legacy_case_id],
                    "legacy_case_id": legacy_case_id,
                    "witness_id": new_witness_id,
                    "legacy_witness_id": legacy_witness_id,
                    "origin": origin["origin"],
                    "event_sha256": event_sha256,
                    "segment_id": segment_id,
                    "source_id": source_row["source_id"],
                    "density_stratum": case["density_stratum"],
                }
            )
        cases.append(
            {
                "case_id": case_id_map[legacy_case_id],
                "source_excerpt": case["source_excerpt"],
                "event_set_a": sorted(
                    rendered["reference"], key=lambda row: row["witness_id"]
                ),
                "event_set_b": sorted(
                    rendered["candidate"], key=lambda row: row["witness_id"]
                ),
            }
        )
    pool = {
        "schema_version": app_server_llm_judge.SHARED_WITNESS_POOL_VERSION,
        "seed_sha256": sha256_text("v224-observed-residual-pool"),
        "cases": cases,
        "privacy": "private_analysis_only_blinded_no_origin_provenance",
    }
    errors = app_server_llm_judge.validate_shared_witness_pool(pool)
    if errors:
        raise JudgeV5SelectionV224Error(
            "v224 normalized witness pool is invalid: " + "; ".join(errors)
        )
    receipts = {
        **predecessor["receipts"],
        "units": [
            {
                **row,
                "case_id": case_id_map[str(row["case_id"])],
                "witness_id": witness_id_map[str(row["witness_id"])],
            }
            for row in predecessor["receipts"]["units"]
        ],
    }
    empty_cases = []
    for row in source_by_segment.values():
        if str(row["segment_id"]) in observed_segments:
            continue
        if row.get("input_events") or row.get("output_events"):
            raise JudgeV5SelectionV224Error("v224 omitted a nonempty observed case")
        empty_cases.append(
            {
                "segment_id": row["segment_id"],
                "source_id": row["source_id"],
                "density_stratum": row["density_stratum"],
            }
        )
    if (
        len(empty_cases) != 1
        or len(source_by_segment) != EXPECTED_OBSERVED_SEGMENT_COUNT
    ):
        raise JudgeV5SelectionV224Error("v224 observed segment partition drifted")
    base = judge.build_neutral_alignment_input(
        pool, receipts, permutation="base"
    )
    canary = judge.build_neutral_alignment_input(
        pool, receipts, permutation="balanced_canary"
    )
    base_case_ids = [row["case_id"] for row in base["cases"]]
    canary_case_ids = [row["case_id"] for row in canary["cases"]]
    if canary_case_ids != list(reversed(base_case_ids)):
        raise JudgeV5SelectionV224Error("v224 case permutation drifted")
    for base_case in base["cases"]:
        canary_case = next(
            row for row in canary["cases"] if row["case_id"] == base_case["case_id"]
        )
        base_ids = [row["witness_id"] for row in base_case["witnesses"]]
        canary_ids = [row["witness_id"] for row in canary_case["witnesses"]]
        if canary_ids != list(reversed(base_ids)):
            raise JudgeV5SelectionV224Error("v224 witness permutation drifted")
    turns = []
    for turn_name, permutation, value in zip(
        TURN_NAMES, PERMUTATIONS, (base, canary), strict=True
    ):
        prompt = v130.alignment_prompt_v130(value)
        schema = judge.neutral_alignment_output_schema(value)
        prompt_bytes = len(prompt.encode("utf-8"))
        schema_bytes = len(_canonical_json(schema).encode("utf-8"))
        if (
            prompt_bytes > MAXIMUM_PROMPT_BYTES
            or schema_bytes > MAXIMUM_SCHEMA_BYTES
        ):
            raise JudgeV5SelectionV224Error(
                "v224 alignment request exceeds frozen size cap"
            )
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
            }
        )
    return {
        "schema_version": V224_POOL_VERSION,
        "pool": pool,
        "receipts": receipts,
        "mapping": {
            "schema_version": V224_POOL_VERSION,
            "rows": sorted(mapping_rows, key=lambda row: row["witness_id"]),
            "empty_cases": empty_cases,
            "privacy": "private_origin_and_id_mapping_no_source_or_event_text",
        },
        "turns": turns,
    }


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(
        bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V224_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v223_terminal": predecessor["records"]["terminal"],
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
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": (
                MINIMUM_REMAINING_RESERVE_PERCENT
            ),
        },
    }
    _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V224_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": (
            MINIMUM_REMAINING_RESERVE_PERCENT
        ),
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_immutable(policy_path, policy)
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "alignment-normalized.private.json",
    }


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v223._expected_runtime_paths())
            | {
                Path(__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(judge.__file__).resolve(),
                Path(v130.__file__).resolve(),
                Path(v222.__file__).resolve(),
                Path(v223.__file__).resolve(),
                Path(app_server_llm_judge.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    predecessor = _validate_v223_success()
    lock = _load_json(path, "v224 runtime lock")
    expected_runtime = {str(item) for item in _expected_runtime_paths()}
    actual_runtime = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping) and isinstance(record.get("path"), str)
    }
    if (
        lock.get("schema_version") != V224_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or actual_runtime != expected_runtime
        or lock.get("semantic_turn_count") != len(TURN_NAMES)
        or lock.get("retry_count_per_turn") != 0
        or lock.get("extraction_replay_allowed") is not False
        or lock.get("holdout_authorized_before_score") is not False
        or lock.get("production_mutation_allowed") is not False
        or lock.get("v223_attempt")
        != list(predecessor["records"].values())
    ):
        raise JudgeV5SelectionV224Error("v224 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v223_attempt") or []),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_requests") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV224Error("v224 runtime lock record drifted")
    reserve.load_reserve_capacity_policy(
        Path(lock["capacity_policy"]["path"])
    )
    return lock


def freeze_v224(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {
            "root": root,
            "terminal": _load_json(terminal_path, "v224 terminal"),
        }
    if any(root.iterdir()):
        spec_path = root / "attempt-spec.json"
        lock_path = root / "runtime-lock.json"
        if not spec_path.exists() or not lock_path.exists():
            raise JudgeV5SelectionV224Error(
                "v224 root is nonempty without a complete presemantic freeze"
            )
        if (root / "launch-receipt.json").exists():
            raise JudgeV5SelectionV224Error(
                "v224 launch exists without terminal; replay is prohibited"
            )
        verify_runtime_lock(lock_path)
        predecessor = _validate_v223_success()
        return {
            "root": root,
            "predecessor": predecessor,
            "spec": _load_json(spec_path, "v224 spec"),
            "spec_path": spec_path,
            "runtime_lock": lock_path,
            "capacity_policy": root / "capacity-policy.json",
            "bundle": _load_frozen_bundle(root),
        }
    predecessor = _validate_v223_success()
    bundle = build_v224_pool(predecessor)
    design = {
        "schema_version": V224_DESIGN_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "frozen_v130_origin_neutral_alignment_two_permutations",
        "case_count": EXPECTED_CASE_COUNT,
        "observed_segment_count": EXPECTED_OBSERVED_SEGMENT_COUNT,
        "witness_count": EXPECTED_WITNESS_COUNT,
        "reference_witness_count": EXPECTED_REFERENCE_WITNESS_COUNT,
        "candidate_witness_count": EXPECTED_CANDIDATE_WITNESS_COUNT,
        "semantic_pruning_performed": False,
        "semantic_matching_performed_by_deterministic_code": False,
        "id_normalization_nonsemantic_only": True,
        "side_labels_in_model_input": False,
        "support_positive_witnesses_only": True,
        "alignment_instruction_hash": sha256_text(
            v130.alignment_instructions_v130()
        ),
        "alignment_model": MODEL,
        "reasoning_effort": EFFORT,
        "semantic_turn_count": len(TURN_NAMES),
        "permutations": list(PERMUTATIONS),
        "retry_count_per_turn": 0,
        "noninferiority_margin": NONINFERIORITY_MARGIN,
        "production_amortized_total_token_ratio_target": TOKEN_RATIO_TARGET,
        "repair_or_adjudication_turn_authorized": False,
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
        "privacy": "opaque_ids_counts_hashes_and_sizes_only",
    }
    design_path = root / "alignment-design.json"
    pool_path = root / "alignment-pool.private.json"
    receipts_path = root / "support-receipts-normalized.private.json"
    mapping_path = root / "origin-map.private.json"
    instructions_path = root / "alignment-instructions.private.md"
    _write_immutable(design_path, design)
    _write_immutable(pool_path, bundle["pool"])
    _write_immutable(receipts_path, bundle["receipts"])
    _write_immutable(mapping_path, bundle["mapping"])
    _write_private_text(instructions_path, v130.alignment_instructions_v130())
    request_records = []
    for turn in bundle["turns"]:
        stem = turn["turn_name"].replace("_", "-")
        input_path = root / f"{stem}-input.private.json"
        prompt_path = root / f"{stem}-prompt.private.md"
        schema_path = root / f"{stem}-schema.json"
        _write_immutable(input_path, turn["value"])
        _write_private_text(prompt_path, turn["prompt"])
        _write_immutable(schema_path, turn["schema"])
        turn["paths"] = {
            "input": input_path,
            "prompt": prompt_path,
            "schema": schema_path,
        }
        request_records.extend(
            [_record(input_path), _record(prompt_path), _record(schema_path)]
        )
    capacity_paths = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V224_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_semantic_attempt",
        "turn_plan": list(TURN_NAMES),
        "declared_turn_count": len(TURN_NAMES),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": TIMEOUT_SECONDS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "maximum_prompt_bytes": MAXIMUM_PROMPT_BYTES,
        "maximum_schema_bytes": MAXIMUM_SCHEMA_BYTES,
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "api_key_billing_allowed": False,
        "raw_session_token_access_allowed": False,
        "codex_exec_semantic_calls_allowed": False,
        "semantic_regex_or_keyword_pruning_allowed": False,
        "extraction_replay_allowed": False,
        "quality_gates_changed": False,
        "noninferiority_margin": NONINFERIORITY_MARGIN,
        "production_amortized_total_token_ratio_target": TOKEN_RATIO_TARGET,
        "repair_or_adjudication_turn_authorized": False,
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
        "v223_attempt": predecessor["records"],
        "frozen_inputs": {
            "design": _record(design_path),
            "pool": _record(pool_path),
            "receipts": _record(receipts_path),
            "mapping": _record(mapping_path),
            "instructions": _record(instructions_path),
            "requests": request_records,
        },
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {
        "schema_version": V224_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _expected_runtime_paths()],
        "v223_attempt": list(predecessor["records"].values()),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_requests": [
            _record(design_path),
            _record(pool_path),
            _record(receipts_path),
            _record(mapping_path),
            _record(instructions_path),
            *request_records,
        ],
        "semantic_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "extraction_replay_allowed": False,
        "holdout_authorized_before_score": False,
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
        "bundle": bundle,
    }


def _load_frozen_bundle(root: Path) -> dict[str, Any]:
    turns = []
    for turn_name, permutation in zip(TURN_NAMES, PERMUTATIONS, strict=True):
        stem = turn_name.replace("_", "-")
        input_path = root / f"{stem}-input.private.json"
        prompt_path = root / f"{stem}-prompt.private.md"
        schema_path = root / f"{stem}-schema.json"
        value = _load_json(input_path, f"v224 {turn_name} input")
        prompt = prompt_path.read_text(encoding="utf-8")
        schema = _load_json(schema_path, f"v224 {turn_name} schema")
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "prompt_bytes": len(prompt.encode("utf-8")),
                "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
                "paths": {
                    "input": input_path,
                    "prompt": prompt_path,
                    "schema": schema_path,
                },
            }
        )
    return {
        "schema_version": V224_POOL_VERSION,
        "pool": _load_json(root / "alignment-pool.private.json", "v224 pool"),
        "receipts": _load_json(
            root / "support-receipts-normalized.private.json",
            "v224 receipts",
        ),
        "mapping": _load_json(root / "origin-map.private.json", "v224 mapping"),
        "turns": turns,
    }


def validate_alignment_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    return judge.validate_neutral_alignment_output(output, alignment_input)


def _alignment_consistency_issues(
    normalized: Mapping[str, Any]
) -> list[dict[str, str]]:
    issues = []
    for case in normalized.get("cases") or []:
        group_by_id = {
            witness_id: index
            for index, group in enumerate(case["equivalence_groups"])
            for witness_id in group
        }
        for pair in case["alignment_pairs"]:
            first, second = pair["witness_ids"]
            same_group = group_by_id[first] == group_by_id[second]
            if (pair["relation"] == "equivalent") != same_group:
                issues.append(
                    {
                        "case_id": case["case_id"],
                        "reason": "relation_equivalence_partition_conflict",
                    }
                )
    return issues


def _case_has_abstention(case: Mapping[str, Any]) -> bool:
    return any(
        pair.get("relation") == "abstain"
        or "abstain" in (pair.get("checklist_decisions") or {}).values()
        for pair in case.get("alignment_pairs") or []
    )


def _f1(precision: float, recall: float) -> float:
    return 1.0 if precision + recall == 0 else 2 * precision * recall / (
        precision + recall
    )


def score_alignment(
    *,
    base: Mapping[str, Any],
    canary: Mapping[str, Any],
    mapping: Mapping[str, Any],
    observed_token_ratio: float,
) -> dict[str, Any]:
    base_rows = {str(row["case_id"]): row for row in base["cases"]}
    canary_rows = {str(row["case_id"]): row for row in canary["cases"]}
    if set(base_rows) != set(canary_rows) or len(base_rows) != EXPECTED_CASE_COUNT:
        raise JudgeV5SelectionV224Error("v224 normalized case coverage drifted")
    mapping_rows = mapping.get("rows") or []
    origin_by_id = {str(row["witness_id"]): str(row["origin"]) for row in mapping_rows}
    metadata_by_case = {}
    for row in mapping_rows:
        metadata_by_case.setdefault(
            str(row["case_id"]),
            {
                "segment_id": row["segment_id"],
                "source_id": row["source_id"],
                "density_stratum": row["density_stratum"],
            },
        )
    disagreement_case_ids = sorted(
        case_id
        for case_id in base_rows
        if v130._owner_projection(base_rows[case_id])
        != v130._owner_projection(canary_rows[case_id])
    )
    consistency_issues = _alignment_consistency_issues(base)
    abstention_case_ids = sorted(
        case_id for case_id, row in base_rows.items() if _case_has_abstention(row)
    )
    case_scores = []
    represented_reference_total = 0
    reference_unit_total = 0
    for case_id, row in sorted(base_rows.items()):
        metadata = metadata_by_case[case_id]
        groups = row["equivalence_groups"]
        group_by_id = {
            witness_id: index
            for index, group in enumerate(groups)
            for witness_id in group
        }
        reference_groups = {
            index
            for index, group in enumerate(groups)
            if any(origin_by_id[witness_id] == "reference" for witness_id in group)
        }
        candidate_groups = {
            index
            for index, group in enumerate(groups)
            if any(origin_by_id[witness_id] == "candidate" for witness_id in group)
        }
        represented = reference_groups & candidate_groups
        for pair in row["alignment_pairs"]:
            if pair["relation"] != "partial":
                continue
            first, second = pair["witness_ids"]
            if {origin_by_id[first], origin_by_id[second]} != {
                "reference",
                "candidate",
            }:
                continue
            reference_id = (
                first if origin_by_id[first] == "reference" else second
            )
            represented.add(group_by_id[reference_id])
        reference_count = len(reference_groups)
        represented_count = len(represented)
        recall = represented_count / reference_count if reference_count else 1.0
        precision = 1.0
        f1 = _f1(precision, recall)
        represented_reference_total += represented_count
        reference_unit_total += reference_count
        case_scores.append(
            {
                "case_id": case_id,
                **metadata,
                "reference_semantic_unit_count": reference_count,
                "candidate_semantic_unit_count": len(candidate_groups),
                "represented_reference_semantic_unit_count": represented_count,
                "source_supported_candidate_precision": precision,
                "reference_unit_recall": round(recall, 6),
                "supported_event_semantic_f1": round(f1, 6),
            }
        )
    for empty in mapping.get("empty_cases") or []:
        case_scores.append(
            {
                "case_id": None,
                **empty,
                "reference_semantic_unit_count": 0,
                "candidate_semantic_unit_count": 0,
                "represented_reference_semantic_unit_count": 0,
                "source_supported_candidate_precision": 1.0,
                "reference_unit_recall": 1.0,
                "supported_event_semantic_f1": 1.0,
            }
        )
    case_scores.sort(key=lambda row: str(row["segment_id"]))
    if len(case_scores) != EXPECTED_OBSERVED_SEGMENT_COUNT:
        raise JudgeV5SelectionV224Error("v224 scored segment count drifted")
    macro_f1 = sum(row["supported_event_semantic_f1"] for row in case_scores) / len(
        case_scores
    )
    source_rows: dict[str, list[float]] = defaultdict(list)
    for row in case_scores:
        source_rows[str(row["source_id"])].append(
            float(row["supported_event_semantic_f1"])
        )
    source_macro = {
        source_id: round(sum(values) / len(values), 6)
        for source_id, values in sorted(source_rows.items())
    }
    checks = {
        "permutation_projection_exact": not disagreement_case_ids,
        "primary_abstention_count_0": not abstention_case_ids,
        "alignment_partition_relation_consistent": not consistency_issues,
        "observed_segment_count_4": len(case_scores)
        == EXPECTED_OBSERVED_SEGMENT_COUNT,
        "dense_segment_count_2": sum(
            row["density_stratum"] == "dense" for row in case_scores
        )
        == 2,
        "no_signal_segment_count_2": sum(
            row["density_stratum"] == "no_signal" for row in case_scores
        )
        == 2,
        "all_candidate_propositions_source_supported": True,
        "exact_evidence_rate_1": True,
        "development_macro_noninferior_margin_0_03": macro_f1
        >= 1.0 - NONINFERIORITY_MARGIN,
        "no_material_source_macro_regression": all(
            value >= 1.0 - NONINFERIORITY_MARGIN
            for value in source_macro.values()
        ),
        "no_signal_source_supported_false_positive_count_0": True,
        "production_amortized_total_token_ratio_lte_0_28": observed_token_ratio
        <= TOKEN_RATIO_TARGET,
    }
    passed = all(checks.values())
    return {
        "schema_version": V224_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "case_count": len(case_scores),
            "aligned_nonempty_case_count": len(base_rows),
            "empty_case_count": len(mapping.get("empty_cases") or []),
            "permutation_exact_case_count": len(base_rows)
            - len(disagreement_case_ids),
            "permutation_case_count": len(base_rows),
            "abstention_case_count": len(abstention_case_ids),
            "alignment_consistency_issue_count": len(consistency_issues),
            "reference_semantic_unit_count": reference_unit_total,
            "represented_reference_semantic_unit_count": (
                represented_reference_total
            ),
            "development_supported_event_semantic_macro_f1": round(
                macro_f1, 6
            ),
            "baseline_reference_macro_f1": 1.0,
            "delta_vs_reference": round(macro_f1 - 1.0, 6),
            "source_macro_f1": source_macro,
            "production_amortized_total_token_ratio": observed_token_ratio,
        },
        "case_scores": case_scores,
        "permutation_disagreement_case_ids": disagreement_case_ids,
        "abstention_case_ids": abstention_case_ids,
        "alignment_consistency_issues": consistency_issues,
        "development_winner_frozen": passed,
        "holdout_authorized": passed,
        "overall_evaluation_complete": False,
        "production_mutated": False,
        "privacy": "sanitized_counts_metrics_and_opaque_ids_no_source_or_event_text",
    }


def _sidecar_accounting(root: Path) -> dict[str, Any]:
    usage = {field: 0 for field in USAGE_FIELDS}
    attempted = measured = unknown = 0
    sidecars = []
    for turn_name in TURN_NAMES:
        paths = _turn_paths(root, turn_name)
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        attempted += 1
        if not paths["sidecar"].exists():
            unknown += 1
            continue
        sidecar = _load_json(paths["sidecar"], f"v224 {turn_name} sidecar")
        sidecars.append(_record(paths["sidecar"]))
        try:
            turn_usage = _validate_usage(sidecar)
            if (
                sidecar.get("usage_status") != "measured"
                or sidecar.get("usage_complete") is not True
            ):
                raise JudgeV5SelectionV224Error("v224 usage is incomplete")
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
    capacity_value = _load_json(paths["capacity"], f"v224 {turn_name} capacity")
    sidecar = _load_json(paths["sidecar"], f"v224 {turn_name} sidecar")
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
        or not paths["output"].is_file()
    ):
        raise JudgeV5SelectionV224Error(
            f"v224 measured turn contract drifted: {turn_name}"
        )
    return usage


def _write_failure(
    *, root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    accounting = _sidecar_accounting(root)
    message = str(exc).encode("utf-8", errors="replace")
    failure = {
        "schema_version": V224_FAILURE_VERSION,
        "failed_at": now_iso(),
        "error_class": type(exc).__name__,
        "error_message_sha256": sha256_text(
            message.decode("utf-8", errors="replace")
        ),
        "error_message_bytes": len(message),
        **accounting,
        "semantic_retry_allowed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "error_class_hash_length_and_aggregate_usage_only",
    }
    failure_path = root / "failure.json"
    _write_stable_time(failure_path, failure, "failed_at")
    predecessor = frozen["predecessor"]["terminal"]
    known = _sum_usage(
        predecessor["cumulative_known_usage_lower_bound"], accounting["usage"]
    )
    unknown = int(predecessor["cumulative_unknown_usage_turn_count"]) + int(
        accounting["unknown_usage_turn_count"]
    )
    upper = int(
        predecessor["cumulative_conservative_unknown_usage_upper_bound"]
    ) + int(accounting["unknown_usage_turn_count"]) * int(
        MAXIMUM_TOTAL_TOKENS_PER_TURN
    )
    terminal = {
        "schema_version": V224_TERMINAL_VERSION,
        "state": "failed",
        "terminal_at": now_iso(),
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
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


async def run_v224(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _default_client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v224 terminal")
    frozen = freeze_v224(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise JudgeV5SelectionV224Error(
            "v224 launch receipt exists; semantic replay is prohibited"
        )
    launch = {
        "schema_version": V224_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "launched_at": now_iso(),
        "declared_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "managed_chatgpt_auth_only": True,
        "semantic_thread_or_turn_started_before_receipt": False,
        "extraction_replay_allowed": False,
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
    }
    _write_immutable(launch_path, launch)
    started = time.monotonic()
    try:
        normalized = []
        async with client_factory(frozen["capacity_policy"]) as client:
            for turn in frozen["bundle"]["turns"]:
                turn_name = turn["turn_name"]
                paths = _turn_paths(root, turn_name)
                paths["root"].mkdir(parents=True, exist_ok=True)
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=v130.alignment_instructions_v130(),
                    prompt=turn["prompt"],
                    output_schema=turn["schema"],
                    cwd=v220.PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=EXPECTED_WITNESS_COUNT,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                    output_validator=lambda candidate, value=turn["value"]: (
                        validate_alignment_output(candidate, value)
                    ),
                )
                if result.status_ok is not True or not isinstance(
                    result.output, Mapping
                ):
                    raise JudgeV5SelectionV224Error(
                        f"v224 turn did not complete: {turn_name}"
                    )
                _validate_measured_turn(root, turn_name)
                projected = judge.normalize_neutral_alignment_output(
                    result.output, turn["value"]
                )
                _write_immutable(paths["normalized"], projected)
                normalized.append(projected)
        accounting = _sidecar_accounting(root)
        if (
            accounting["attempted_turn_count"] != len(TURN_NAMES)
            or accounting["measured_turn_count"] != len(TURN_NAMES)
            or accounting["unknown_usage_turn_count"] != 0
            or accounting["accounting_complete"] is not True
        ):
            raise JudgeV5SelectionV224Error("v224 accounting is incomplete")
        observed_token_ratio = float(
            frozen["predecessor"]["v222"]["gate"][
                "observed_production_amortized_total_token_ratio"
            ]
        )
        score = score_alignment(
            base=normalized[0],
            canary=normalized[1],
            mapping=frozen["bundle"]["mapping"],
            observed_token_ratio=observed_token_ratio,
        )
        score_path = root / "alignment-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            winner = {
                "schema_version": V224_WINNER_VERSION,
                "frozen_at": now_iso(),
                "system_id": "fresh_integrated_base_v220_v224_frozen",
                "configuration": {
                    "model": v220.MODEL,
                    "reasoning_effort": v220.EFFORT,
                    "max_events_per_segment": v220.MAX_EVENTS_PER_SEGMENT,
                    "design": _record(
                        v220.DEFAULT_OUTPUT_ROOT / "integrated-base-design.json"
                    ),
                    "attempt_spec": _record(
                        v220.DEFAULT_OUTPUT_ROOT / "attempt-spec.json"
                    ),
                    "instructions": _record(
                        v220.DEFAULT_OUTPUT_ROOT
                        / "integrated-core-instructions.private.md"
                    ),
                },
                "development_evidence": {
                    "v222": frozen["predecessor"]["v222"]["records"],
                    "v223": frozen["predecessor"]["records"],
                    "v224_score": _record(score_path),
                },
                "development_quality_passed": True,
                "production_amortized_total_token_ratio": observed_token_ratio,
                "production_amortized_token_target_passed": True,
                "untouched_holdout_authorized": True,
                "overall_evaluation_complete": False,
                "production_mutated": False,
                "privacy": (
                    "hashes_counts_metrics_and_configuration_"
                    "no_source_or_event_text"
                ),
            }
            _write_stable_time(winner_path, winner, "frozen_at")
            winner_record = _record(winner_path)
        predecessor_terminal = frozen["predecessor"]["terminal"]
        cumulative = _sum_usage(
            predecessor_terminal["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        terminal = {
            "schema_version": V224_TERMINAL_VERSION,
            "state": "completed" if passed else "development_strategy_not_accepted",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v224_alignment_passed_development_winner_frozen_holdout_authorized"
                if passed
                else "v224_alignment_quality_or_permutation_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
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
            "cumulative_unknown_usage_turn_count": predecessor_terminal[
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": (
                predecessor_terminal[
                    "cumulative_conservative_unknown_usage_upper_bound"
                ]
            ),
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "development_winner": winner_record,
            "sidecars": accounting["sidecars"],
            "spec": _record(frozen["spec_path"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "launch_receipt": _record(launch_path),
            "required_next_artifact_path": str(
                root.parent
                / (
                    "untouched-holdout-v5_4-v225-frozen-winner-execution"
                    if passed
                    else "development-selection-v5_4-v225-alignment-nonacceptance"
                )
                / "terminal.json"
            ),
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except BaseException as exc:
        if terminal_path.exists():
            return _load_json(terminal_path, "v224 terminal")
        return _write_failure(root=root, frozen=frozen, exc=exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run v224 frozen residual alignment audit"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v224(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "development_winner_frozen": terminal[
                    "development_winner_frozen"
                ],
                "holdout_authorized": terminal["holdout_authorized"],
                "usage_status": terminal["usage_status"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
