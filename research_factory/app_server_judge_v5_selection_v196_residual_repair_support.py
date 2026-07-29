from __future__ import annotations

"""Run frozen side-free support over the v195 repaired events."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_selection_v175_support as v175
from . import app_server_judge_v5_selection_v195_metric_patch_adoption as v195
from .app_server_judge_v5 import build_pointwise_support_input
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_llm_judge import make_shared_witness_pool
from .util import now_iso, sha256_text


V196_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v196_spec_v1"
V196_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v196_terminal_v1"
V196_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v196_failure_v1"
V196_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V196_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V196_PHASE_ID = "judge_v5_4_selection_v196_residual_repair_support"

TURN_NAME = "selection_residual_repair_support_00"
MODEL = v175.MODEL
EFFORT = v175.EFFORT
MAXIMUM_TOTAL_TOKENS_PER_TURN = 80_000
TIMEOUT_SECONDS = 1200.0
POOL_SEED = "pif-v196-residual-repair-support-v1"
DEFAULT_OUTPUT_ROOT = (
    v195.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v196-residual-repair-support"
).resolve()


class JudgeV5SelectionV196Error(RuntimeError):
    """The residual-repair support pass cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v195_authorization() -> dict[str, Any]:
    root = v195.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    spec_path = root / "metric-patch-adoption-spec.json"
    gate_path = root / "metric-patch-adoption-gate.json"
    output_path = root / "residual-repair-metric-patched-output.private.json"
    diagnostics_path = root / "metric-patched-structural-diagnostics.json"
    terminal = _load_json(terminal_path, "v195 terminal")
    spec = _load_json(spec_path, "v195 spec")
    gate = _load_json(gate_path, "v195 gate")
    output = _load_json(output_path, "v195 repaired output")
    diagnostics = _load_json(diagnostics_path, "v195 diagnostics")
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v195_adopted_metric_patch_cleared_phase_one_gates_support_judge_authorized"
        or terminal.get("v194_turn_replayed") is not False
        or terminal.get("v194_failure_preserved") is not True
        or terminal.get("support_judge_authorized") is not True
        or terminal.get("alignment_judge_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("attempt_usage", {}).get("total_tokens") != 0
        or terminal.get("combined_repair_usage", {}).get("total_tokens") != 73487
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8367769
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or terminal.get("repaired_output") != _record(output_path)
        or terminal.get("structural_diagnostics") != _record(diagnostics_path)
        or terminal.get("phase_one_gate") != _record(gate_path)
        or terminal.get("spec") != _record(spec_path)
        or spec.get("state") != "zero_model_call_adoption_completed"
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("v194_turn_replayed") is not False
        or gate.get("passed") is not True
        or gate.get("event_count") != 30
        or gate.get("exactness_pruned_event_count") != 0
        or gate.get("metric_grounding_error_event_count") != 0
        or gate.get("no_signal_false_positive_event_count") != 0
        or gate.get("production_amortized_total_token_ratio") != 0.236727
        or len(output.get("segments") or []) != 5
        or sum(len(row.get("events") or []) for row in output["segments"]) != 30
    ):
        raise JudgeV5SelectionV196Error("v195 support authorization contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV196Error("v195 runtime binding drifted")
    predecessor = v195._validate_v194_completed_failed_attempt()
    if terminal.get("cumulative_known_usage_lower_bound") != predecessor["terminal"].get(
        "cumulative_known_usage_lower_bound"
    ):
        raise JudgeV5SelectionV196Error("v195 cumulative accounting drifted")
    return {
        "root": root,
        "terminal": terminal,
        "spec": spec,
        "gate": gate,
        "output": output,
        "diagnostics": diagnostics,
        "predecessor": predecessor,
        "records": {
            "terminal": _record(terminal_path),
            "spec": _record(spec_path),
            "gate": _record(gate_path),
            "output": _record(output_path),
            "diagnostics": _record(diagnostics_path),
        },
    }


def _repair_pool(predecessor: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    source_cases = {
        str(row["case_id"]): row
        for row in predecessor["predecessor"]["predecessor"]["predecessor"][
            "private_input"
        ]["cases"]
    }
    raw_cases = []
    for segment in predecessor["output"]["segments"]:
        case_id = str(segment["segment_id"])
        source = source_cases[case_id]
        raw_cases.append(
            {
                "case_key": case_id,
                "source_excerpt": source["source_excerpt"],
                "event_set_a": [],
                "event_set_b": [
                    {
                        "event": event,
                        "provenance": {
                            "original_case_id": case_id,
                            "repair_event_index": index,
                            "repair_system_id": "residual_repair_batch8_pair_v195",
                            "submitted_evidence_exact": True,
                        },
                    }
                    for index, event in enumerate(segment.get("events") or [])
                ],
                "provenance": {
                    "original_case_id": case_id,
                    "source_id": source["source_id"],
                    "density_stratum": source["density_stratum"],
                    "case_key": source["case_key"],
                },
            }
        )
    pool, mapping = make_shared_witness_pool(raw_cases, seed=POOL_SEED)
    pointwise = build_pointwise_support_input(pool)
    if len(pool["cases"]) != 5 or len(pointwise["units"]) != 30:
        raise JudgeV5SelectionV196Error("v196 repair witness coverage drifted")
    return pool, mapping


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAXIMUM_TOTAL_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V196_CAPACITY_AUDIT_VERSION,
        "phase_id": V196_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v195_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor["terminal"][
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": 1,
            "predecessor_unknown_usage_upper_bound": 120000,
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V196_CAPACITY_POLICY_VERSION,
        "phase_id": V196_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v196(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v196 terminal")}
    predecessor = _validate_v195_authorization()
    pool, mapping = _repair_pool(predecessor)
    pointwise = build_pointwise_support_input(pool)
    value = v175._support_value(pointwise["units"])
    prompt = v175.compact_support_prompt(pointwise["units"])
    schema = v143.support_output_schema(value)
    pool_path = root / "residual-repair-witness-pool.private.json"
    mapping_path = root / "residual-repair-witness-mapping.private.json"
    _write_immutable(pool_path, pool)
    _write_immutable(mapping_path, mapping)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=value,
        prompt=prompt,
        schema=schema,
    )
    capacity = _build_capacity_policy(root, predecessor)
    instructions = v143.support_base_instructions_v143()
    spec = {
        "schema_version": V196_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": V196_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "frozen_side_free_pointwise_support_for_residual_repair_events",
        "turn_plan": [TURN_NAME],
        "case_count": 5,
        "witness_count": 30,
        "retry_count_per_turn": 0,
        "support_rubric_changed": False,
        "system_identity_present": False,
        "side_labels_present": False,
        "alignment_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v195.__file__)),
            _record(Path(v175.__file__)),
            _record(Path(v143.__file__)),
        ],
        "frozen_instructions": {
            "support_base_sha256": sha256_text(instructions),
        },
        "frozen_inputs": {
            "pool": _record(pool_path),
            "mapping": _record(mapping_path),
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "residual-repair-support-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "paths": paths,
        "predecessor": predecessor,
        "pool": pool,
        "mapping": mapping,
        "value": value,
        "prompt": prompt,
        "schema": schema,
        "instructions": instructions,
    }


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], error_class: str
) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v196 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative_lower = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], usage
    )
    failure = {
        "schema_version": V196_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
        "cumulative_known_usage_lower_bound": cumulative_lower,
        "cumulative_unknown_usage_turn_count": 1 + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": 120000
        + unknown * MAXIMUM_TOTAL_TOKENS_PER_TURN,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V196_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "support_receipts_frozen": False,
        "alignment_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": cumulative_lower,
        "cumulative_unknown_usage_turn_count": 1 + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": failure[
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v196(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v196 terminal")
    frozen = freeze_v196(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=frozen["instructions"],
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=30,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: v143.validate_support_output(
                    candidate, frozen["value"]
                ),
            )
        receipts_path = root / "residual-repair-support-receipts.private.json"
        _write_immutable(receipts_path, output)
        accounting = _aggregate_usage([sidecar])
        cumulative_lower = _sum_usage(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        verdict_counts: dict[str, int] = {}
        field_counts: dict[str, int] = {}
        for row in output["units"]:
            verdict = str(row["proposition_verdict"])
            verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
            field_verdict = str(row["structured_field_verdict"])
            field_counts[field_verdict] = field_counts.get(field_verdict, 0) + 1
        terminal = {
            "schema_version": V196_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v196_residual_repair_support_completed_alignment_authorized",
            "overall_evaluation_complete": False,
            "support_receipts": _record(receipts_path),
            "support_receipts_frozen": True,
            "support_verdict_counts": dict(sorted(verdict_counts.items())),
            "structured_field_verdict_counts": dict(sorted(field_counts.items())),
            "alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "cumulative_usage_status": "unknown",
            "cumulative_known_usage_lower_bound": cumulative_lower,
            "cumulative_unknown_usage_turn_count": 1,
            "cumulative_conservative_unknown_usage_upper_bound": 120000,
            "required_next_artifact_path": str(
                root.parent
                / "development-selection-v5_4-v197-residual-repair-alignment"
                / "terminal.json"
            ),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, frozen["predecessor"], exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["predecessor"], type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v196 residual-repair support")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v196(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "support_receipts_frozen": terminal.get("support_receipts_frozen", False),
                "alignment_authorized": terminal.get("alignment_authorized", False),
                "usage_status": terminal.get("usage_status"),
                "holdout_authorized": terminal.get("holdout_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
