from __future__ import annotations

"""Run the one-turn v192 residual-repair diagnostic."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from .app_server_evaluation import normalize_episode_batch_output
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
from .labels import ValidationError, _validate_schema
from .util import now_iso


V193_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v193_spec_v1"
V193_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v193_gate_v1"
V193_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v193_terminal_v1"
V193_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v193_failure_v1"
V193_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V193_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V193_PHASE_ID = "judge_v5_4_selection_v193_residual_repair_diagnostic"

TURN_NAME = "selection_residual_repair_diagnostic_00"
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    v192.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v193-residual-repair-diagnostic"
).resolve()


class JudgeV5SelectionV193Error(RuntimeError):
    """The residual-repair semantic attempt cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v192_authorization() -> dict[str, Any]:
    root = v192.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    design_path = root / "residual-repair-design.json"
    oracle_path = root / "residual-repair-oracle-audit.json"
    terminal = _load_json(terminal_path, "v192 terminal")
    design = _load_json(design_path, "v192 design")
    oracle = _load_json(oracle_path, "v192 oracle audit")
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v192_residual_repair_diagnostic_frozen_semantic_attempt_authorized"
        or terminal.get("semantic_attempt_authorized") is not True
        or terminal.get("authorized_turn_count") != 1
        or terminal.get("authorized_model") != v192.MODEL
        or terminal.get("authorized_effort") != v192.EFFORT
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("attempt_usage", {}).get("total_tokens") != 0
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8294282
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or terminal.get("design") != _record(design_path)
        or terminal.get("oracle_audit") != _record(oracle_path)
        or design.get("state") != "frozen_before_semantic_attempt"
        or design.get("declared_turn_count") != 1
        or design.get("retry_count_per_turn") != 0
        or design.get("existing_extraction_outputs_reused_only") is not True
        or design.get("not_an_extraction_replay") is not True
        or design.get("batch_5_replayed") is not False
        or design.get("model") != v192.MODEL
        or design.get("reasoning_effort") != v192.EFFORT
        or design.get("maximum_total_tokens_per_turn")
        != v192.MAXIMUM_TOTAL_TOKENS_PER_TURN
        or design.get("repair_budget", {}).get("declared_bound_fits_production_headroom")
        is not True
        or design.get("holdout_authorized") is not False
        or design.get("production_mutation_allowed") is not False
        or oracle.get("bootstrap", {}).get("ci_lower") < -0.03
        or oracle.get("worst_source_delta") < -0.05
        or oracle.get("production_semantic_routing_by_reference_forbidden") is not True
    ):
        raise JudgeV5SelectionV193Error("v192 authorization contract drifted")
    for record in design.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV193Error("v192 runtime binding drifted")
    frozen = design.get("frozen_inputs") or {}
    for key in ("private_input", "prompt", "instructions", "schema"):
        if not _verify_record(frozen.get(key) or {}):
            raise JudgeV5SelectionV193Error(f"v192 frozen {key} drifted")
    predecessor = v192._validate_v191_nonacceptance()
    if terminal.get("cumulative_known_usage_lower_bound") != predecessor["terminal"].get(
        "cumulative_known_usage_lower_bound"
    ):
        raise JudgeV5SelectionV193Error("v192 cumulative accounting drifted")
    private_input = _load_json(Path(frozen["private_input"]["path"]), "v192 private input")
    schema = _load_json(Path(frozen["schema"]["path"]), "v192 repair schema")
    prompt = Path(frozen["prompt"]["path"]).read_text(encoding="utf-8")
    instructions = Path(frozen["instructions"]["path"]).read_text(encoding="utf-8")
    if (
        len(private_input.get("cases") or []) != v192.DIAGNOSTIC_CASE_COUNT
        or len(prompt.encode("utf-8")) != design.get("prompt_bytes")
        or len(instructions.encode("utf-8")) != design.get("instructions_bytes")
        or len(json.dumps(schema, ensure_ascii=True).encode("utf-8"))
        != design.get("schema_bytes")
    ):
        raise JudgeV5SelectionV193Error("v192 frozen packet metadata drifted")
    return {
        "root": root,
        "terminal": terminal,
        "design": design,
        "oracle": oracle,
        "records": {
            "terminal": _record(terminal_path),
            "design": _record(design_path),
            "oracle": _record(oracle_path),
        },
        "predecessor": predecessor,
        "private_input": private_input,
        "prompt": prompt,
        "instructions": instructions,
        "schema": schema,
    }


def validate_repair_envelope(output: Any, schema: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, dict):
        return ["output_not_object"]
    try:
        _validate_schema(dict(schema), output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        return [f"schema:{type(exc).__name__}"]
    expected = [
        item["enum"][0]
        for item in [
            schema["properties"]["segments"]["items"]["properties"]["segment_id"]
        ]
    ]
    actual = [row.get("segment_id") for row in output.get("segments") or []]
    schema_expected = list(
        schema["properties"]["segments"]["items"]["properties"]["segment_id"]["enum"]
    )
    if actual != schema_expected:
        return ["segment_order_or_coverage"]
    if output.get("episode_id") != v192.DIAGNOSTIC_EPISODE_ID:
        return ["episode_id"]
    if expected and expected[0] != schema_expected[0]:
        return ["schema_segment_identity"]
    return []


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = v192.MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V193_CAPACITY_AUDIT_VERSION,
        "phase_id": V193_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v192_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor["terminal"][
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": 1,
            "predecessor_unknown_usage_upper_bound": 120000,
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": bound,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V193_CAPACITY_POLICY_VERSION,
        "phase_id": V193_PHASE_ID,
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
        "maximum_total_tokens_per_turn": bound,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v193(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v193 terminal")}
    predecessor = _validate_v192_authorization()
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=predecessor["private_input"],
        prompt=predecessor["prompt"],
        schema=predecessor["schema"],
    )
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V193_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": V193_PHASE_ID,
        "model": v192.MODEL,
        "reasoning_effort": v192.EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_turn_source_grounded_residual_repair_diagnostic",
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "existing_extraction_outputs_reused_only": True,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "maximum_total_tokens_per_turn": v192.MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "support_or_alignment_judge_calls_in_this_attempt": 0,
        "support_judge_authorized_only_after_phase_one_gate": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v192.__file__)),
        ],
        "frozen_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
            "instructions_sha256": predecessor["design"]["frozen_inputs"][
                "instructions_sha256"
            ],
        },
        "privacy": "private_source_and_events_sanitized_reports_counts_hashes_metrics_only",
    }
    spec_path = root / "residual-repair-attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "paths": paths,
        "predecessor": predecessor,
    }


def _phase_one_gate(
    *, normalized: Mapping[str, Any], diagnostics: Sequence[Mapping[str, Any]], usage: Mapping[str, int], predecessor: Mapping[str, Any]
) -> dict[str, Any]:
    cases = predecessor["private_input"]["cases"]
    rows = normalized.get("segments") or []
    by_id = {str(row["segment_id"]): row for row in rows}
    no_signal_ids = {
        str(row["case_id"]) for row in cases if row["density_stratum"] == "no_signal"
    }
    event_count = sum(len(row.get("events") or []) for row in rows)
    no_signal_events = sum(
        len(by_id[case_id].get("events") or []) for case_id in no_signal_ids
    )
    exactness_pruned = sum(int(row["exactness_pruned_events"]) for row in diagnostics)
    metric_errors = sum(int(row["metric_grounding_error_events"]) for row in diagnostics)
    cap_hits = sum(bool(row["event_cap_hit"]) for row in diagnostics)
    budget = predecessor["design"]["repair_budget"]
    repair_scaled = int(usage["total_tokens"]) * (
        int(budget["diagnostic_case_count"]) and 60 // int(budget["diagnostic_case_count"])
    )
    candidate_end = (
        int(budget["shared_context_tokens"])
        + int(budget["base_extraction_tokens_scaled_to_60_segments"])
        + repair_scaled
    )
    baseline_end = int(budget["baseline_end_to_end_tokens"])
    ratio = candidate_end / baseline_end
    checks = {
        "schema_and_status_success": all(bool(row["status_ok"]) for row in diagnostics),
        "exact_evidence_rate": exactness_pruned == 0,
        "metric_grounding": metric_errors == 0,
        "no_signal_false_positive_events": no_signal_events == 0,
        "event_cap": all(len(row.get("events") or []) <= v192.MAX_EVENTS_PER_CASE for row in rows),
        "declared_turn_token_bound": int(usage["total_tokens"])
        <= v192.MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "production_amortized_total_token_ratio": candidate_end * 25
        <= baseline_end * 7,
    }
    return {
        "schema_version": V193_GATE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "case_count": len(rows),
        "event_count": event_count,
        "no_signal_case_count": len(no_signal_ids),
        "no_signal_false_positive_event_count": no_signal_events,
        "exactness_pruned_event_count": exactness_pruned,
        "metric_grounding_error_event_count": metric_errors,
        "event_cap_hit_case_count": cap_hits,
        "repair_usage": dict(usage),
        "repair_tokens_scaled_to_60_segments": repair_scaled,
        "candidate_end_to_end_tokens": candidate_end,
        "baseline_end_to_end_tokens": baseline_end,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "support_judge_authorized": all(checks.values()),
        "holdout_authorized": False,
        "production_mutated": False,
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v193 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative_lower = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], usage
    )
    failure = {
        "schema_version": V193_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_semantic_attempt_failed",
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
        + unknown * v192.MAXIMUM_TOTAL_TOKENS_PER_TURN,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V193_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_semantic_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "support_judge_authorized": False,
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


async def run_v193(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v193 terminal")
    frozen = freeze_v193(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["predecessor"]["prompt"],
                schema=frozen["predecessor"]["schema"],
                base_instructions=frozen["predecessor"]["instructions"],
                model=v192.MODEL,
                effort=v192.EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=v192.DIAGNOSTIC_CASE_COUNT,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_repair_envelope(
                    candidate, frozen["predecessor"]["schema"]
                ),
            )
        prepared = [
            {
                "segment_id": row["case_id"],
                "segment_text": row["source_excerpt"],
                "boundaries": row["boundaries"],
            }
            for row in frozen["predecessor"]["private_input"]["cases"]
        ]
        normalized, diagnostics = normalize_episode_batch_output(
            output,
            episode_id=v192.DIAGNOSTIC_EPISODE_ID,
            prepared_segments=prepared,
            max_events_per_segment=v192.MAX_EVENTS_PER_CASE,
        )
        normalized_path = root / "residual-repair-output.private.json"
        diagnostics_path = root / "residual-repair-structural-diagnostics.json"
        _write_immutable(normalized_path, normalized)
        _write_immutable(diagnostics_path, {"segments": diagnostics})
        accounting = _aggregate_usage([sidecar])
        gate = _phase_one_gate(
            normalized=normalized,
            diagnostics=diagnostics,
            usage=accounting["usage"],
            predecessor=frozen["predecessor"],
        )
        gate_path = root / "residual-repair-phase-one-gate.json"
        _write_immutable(gate_path, gate)
        cumulative_lower = _sum_usage(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        terminal = {
            "schema_version": V193_TERMINAL_VERSION,
            "state": "completed" if gate["passed"] else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v193_residual_repair_structural_and_token_gates_passed_support_judge_authorized"
                if gate["passed"]
                else "v193_residual_repair_structural_or_token_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "repair_output": _record(normalized_path),
            "structural_diagnostics": _record(diagnostics_path),
            "phase_one_gate": _record(gate_path),
            "support_judge_authorized": gate["passed"],
            "alignment_judge_authorized": False,
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
                / "development-selection-v5_4-v194-residual-repair-support"
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
    parser = argparse.ArgumentParser(description="Run v193 residual-repair diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v193(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "support_judge_authorized": terminal.get("support_judge_authorized", False),
                "usage_status": terminal.get("usage_status"),
                "holdout_authorized": terminal.get("holdout_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
