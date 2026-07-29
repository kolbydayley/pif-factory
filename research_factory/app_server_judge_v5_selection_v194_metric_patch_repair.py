from __future__ import annotations

"""Repair only the five observable v193 metric-grounding failures."""

import argparse
import asyncio
import copy
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v193_residual_repair_diagnostic as v193
from .app_server_evaluation import _metric_grounding_errors, normalize_episode_batch_output
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
from .util import now_iso, sha256_text


V194_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v194_spec_v1"
V194_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v194_gate_v1"
V194_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v194_terminal_v1"
V194_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v194_failure_v1"
V194_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V194_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V194_PHASE_ID = "judge_v5_4_selection_v194_metric_patch_repair"

TURN_NAME = "selection_residual_repair_metric_patch_00"
MODEL = v192.MODEL
EFFORT = v192.EFFORT
MAXIMUM_TOTAL_TOKENS_PER_TURN = 20_000
TIMEOUT_SECONDS = 600.0
DEFAULT_OUTPUT_ROOT = (
    v193.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v194-metric-patch-repair"
).resolve()


class JudgeV5SelectionV194Error(RuntimeError):
    """The metric-only repair cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v193_metric_failure() -> dict[str, Any]:
    root = v193.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    spec_path = root / "residual-repair-attempt-spec.json"
    gate_path = root / "residual-repair-phase-one-gate.json"
    output_path = root / "residual-repair-output.private.json"
    diagnostics_path = root / "residual-repair-structural-diagnostics.json"
    turn_root = root / "turns" / v193.TURN_NAME.replace("_", "-")
    sidecar_path = turn_root / "sidecar.json"
    capacity_path = turn_root / "capacity.json"
    terminal = _load_json(terminal_path, "v193 terminal")
    spec = _load_json(spec_path, "v193 spec")
    gate = _load_json(gate_path, "v193 phase-one gate")
    output = _load_json(output_path, "v193 normalized repair output")
    diagnostics = _load_json(diagnostics_path, "v193 diagnostics")
    sidecar = _load_json(sidecar_path, "v193 sidecar")
    capacity = _load_json(capacity_path, "v193 capacity")
    usage = _validate_usage(sidecar)
    expected_checks = {
        "declared_turn_token_bound": True,
        "event_cap": True,
        "exact_evidence_rate": True,
        "metric_grounding": False,
        "no_signal_false_positive_events": True,
        "production_amortized_total_token_ratio": True,
        "schema_and_status_success": False,
    }
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason")
        != "v193_residual_repair_structural_or_token_gate_not_passed"
        or terminal.get("support_judge_authorized") is not False
        or terminal.get("alignment_judge_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != usage
        or terminal.get("repair_output") != _record(output_path)
        or terminal.get("structural_diagnostics") != _record(diagnostics_path)
        or terminal.get("phase_one_gate") != _record(gate_path)
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8345401
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or spec.get("state") != "frozen_before_model_call"
        or spec.get("turn_plan") != [v193.TURN_NAME]
        or spec.get("retry_count_per_turn") != 0
        or spec.get("existing_extraction_outputs_reused_only") is not True
        or spec.get("extraction_replay_allowed") is not False
        or spec.get("support_or_alignment_judge_calls_in_this_attempt") != 0
        or gate.get("passed") is not False
        or gate.get("checks") != expected_checks
        or gate.get("metric_grounding_error_event_count") != 5
        or gate.get("exactness_pruned_event_count") != 0
        or gate.get("no_signal_false_positive_event_count") != 0
        or gate.get("production_amortized_total_token_ratio") != 0.21006
        or sidecar.get("state") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("cli_version") != "0.144.1"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise JudgeV5SelectionV194Error("v193 metric-failure contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV194Error("v193 runtime binding drifted")
    predecessor = v193._validate_v192_authorization()
    expected_cumulative = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], usage
    )
    if terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative:
        raise JudgeV5SelectionV194Error("v193 cumulative accounting drifted")
    return {
        "root": root,
        "terminal": terminal,
        "spec": spec,
        "gate": gate,
        "output": output,
        "diagnostics": diagnostics,
        "usage": usage,
        "predecessor": predecessor,
        "records": {
            "terminal": _record(terminal_path),
            "spec": _record(spec_path),
            "gate": _record(gate_path),
            "output": _record(output_path),
            "diagnostics": _record(diagnostics_path),
            "sidecar": _record(sidecar_path),
            "capacity": _record(capacity_path),
        },
    }


def _metric_repair_packets(predecessor: Mapping[str, Any]) -> list[dict[str, Any]]:
    output_by_id = {
        str(row["segment_id"]): row for row in predecessor["output"]["segments"]
    }
    packets = []
    for diagnostic in predecessor["diagnostics"]["segments"]:
        case_id = str(diagnostic["segment_id"])
        events = output_by_id[case_id]["events"]
        for row in diagnostic.get("metric_grounding_errors") or []:
            event_index = int(row["event_index"])
            event = events[event_index]
            repair_id = "mrepair_" + sha256_text(f"{case_id}:{event_index}")[:24]
            packets.append(
                {
                    "repair_id": repair_id,
                    "case_id": case_id,
                    "event_index": event_index,
                    "evidence": event["evidence"],
                    "current_metric": {
                        key: event[key]
                        for key in (
                            "metric_value",
                            "metric_unit",
                            "metric_comparator",
                            "metric_direction",
                            "metric_raw_text",
                        )
                    },
                    "observed_error_classes": list(row["errors"]),
                }
            )
    packets.sort(key=lambda row: (row["case_id"], row["event_index"]))
    if len(packets) != 5 or len({row["repair_id"] for row in packets}) != 5:
        raise JudgeV5SelectionV194Error("v193 metric repair coverage drifted")
    return packets


def metric_patch_instructions() -> str:
    return """You repair only literal metric fields for five already-frozen podcast events.

Each packet includes an exact evidence substring and the event's current metric fields. Do not
change, reinterpret, drop, merge, or add any event, claim, actor, stance, target, evidence, or
other field. For each repair_id, return exactly one metric patch.

Use action=patch only when metric_raw_text is a nonempty exact contiguous substring of evidence.
Every nonempty metric_value, metric_unit, and metric_comparator must appear literally in evidence
or metric_raw_text. Choose metric_direction only from the schema enum. If the evidence does not
literally support a metric, use action=clear, set value/unit/comparator/raw_text to empty strings,
and set direction to not_applicable. Preserve repair_id and order exactly. Return JSON only."""


def metric_patch_prompt(packets: Sequence[Mapping[str, Any]]) -> str:
    return (
        "# Metric-only repair packets\n"
        + json.dumps(list(packets), ensure_ascii=True, separators=(",", ":"))
        + "\n"
    )


def metric_patch_schema(packets: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ids = [str(row["repair_id"]) for row in packets]
    patch = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "repair_id",
            "action",
            "metric_value",
            "metric_unit",
            "metric_comparator",
            "metric_direction",
            "metric_raw_text",
        ],
        "properties": {
            "repair_id": {"type": "string", "enum": ids},
            "action": {"type": "string", "enum": ["patch", "clear"]},
            "metric_value": {"type": "string"},
            "metric_unit": {"type": "string"},
            "metric_comparator": {"type": "string"},
            "metric_direction": {
                "type": "string",
                "enum": [
                    "increase",
                    "decrease",
                    "stable",
                    "mixed",
                    "not_applicable",
                    "unknown",
                ],
            },
            "metric_raw_text": {"type": "string"},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["patches"],
        "properties": {
            "patches": {
                "type": "array",
                "minItems": len(ids),
                "maxItems": len(ids),
                "items": patch,
            }
        },
    }


def validate_metric_patch_output(
    output: Any, packets: Sequence[Mapping[str, Any]]
) -> list[str]:
    if not isinstance(output, dict) or not isinstance(output.get("patches"), list):
        return ["patches_missing"]
    patches = output["patches"]
    expected_ids = [str(row["repair_id"]) for row in packets]
    if [row.get("repair_id") for row in patches if isinstance(row, dict)] != expected_ids:
        return ["repair_id_order_or_coverage"]
    by_id = {str(row["repair_id"]): row for row in packets}
    errors = []
    for patch in patches:
        if not isinstance(patch, dict):
            errors.append("patch_not_object")
            continue
        packet = by_id[str(patch["repair_id"])]
        evidence = str(packet["evidence"])
        action = patch.get("action")
        raw = str(patch.get("metric_raw_text") or "")
        components = [
            str(patch.get(key) or "")
            for key in ("metric_value", "metric_unit", "metric_comparator")
        ]
        if action == "clear":
            if any(components) or raw or patch.get("metric_direction") != "not_applicable":
                errors.append(f"{patch['repair_id']}:clear_contract")
        elif action == "patch":
            if not raw or raw not in evidence:
                errors.append(f"{patch['repair_id']}:raw_not_exact")
            for value in components:
                if value and value not in evidence and value not in raw:
                    errors.append(f"{patch['repair_id']}:component_not_exact")
        else:
            errors.append(f"{patch.get('repair_id')}:action")
    return errors


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAXIMUM_TOTAL_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V194_CAPACITY_AUDIT_VERSION,
        "phase_id": V194_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v193_terminal": predecessor["records"]["terminal"],
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
        "schema_version": V194_CAPACITY_POLICY_VERSION,
        "phase_id": V194_PHASE_ID,
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


def freeze_v194(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v194 terminal")}
    predecessor = _validate_v193_metric_failure()
    packets = _metric_repair_packets(predecessor)
    instructions = metric_patch_instructions()
    prompt = metric_patch_prompt(packets)
    schema = metric_patch_schema(packets)
    input_value = {
        "schema_version": "pif_app_server_metric_patch_input_v1",
        "packets": packets,
    }
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=input_value,
        prompt=prompt,
        schema=schema,
    )
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V194_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": V194_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "observable_failure_metric_field_subset_patch",
        "turn_plan": [TURN_NAME],
        "repair_packet_count": len(packets),
        "retry_count_per_turn": 0,
        "event_claim_or_evidence_changes_allowed": False,
        "metric_fields_only": True,
        "extraction_replay_allowed": False,
        "support_or_alignment_judge_calls_in_this_attempt": 0,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v193.__file__)),
            _record(Path(v192.__file__)),
        ],
        "frozen_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
            "instructions_sha256": sha256_text(instructions),
        },
        "privacy": "private_exact_evidence_and_metric_fields_sanitized_reports_only",
    }
    spec_path = root / "metric-patch-attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "paths": paths,
        "predecessor": predecessor,
        "packets": packets,
        "instructions": instructions,
        "prompt": prompt,
        "schema": schema,
    }


def _apply_patches(
    *, output: Mapping[str, Any], packets: Sequence[Mapping[str, Any]], patches: Mapping[str, Any]
) -> dict[str, Any]:
    repaired = copy.deepcopy(output)
    segments = {str(row["segment_id"]): row for row in repaired["segments"]}
    by_id = {str(row["repair_id"]): row for row in packets}
    for patch in patches["patches"]:
        packet = by_id[str(patch["repair_id"])]
        event = segments[str(packet["case_id"])]["events"][int(packet["event_index"])]
        before = {
            key: value for key, value in event.items() if not key.startswith("metric_")
        }
        for key in (
            "metric_value",
            "metric_unit",
            "metric_comparator",
            "metric_direction",
            "metric_raw_text",
        ):
            event[key] = patch[key]
        after = {
            key: value for key, value in event.items() if not key.startswith("metric_")
        }
        if before != after or _metric_grounding_errors(event):
            raise JudgeV5SelectionV194Error("metric patch changed forbidden fields or stayed invalid")
    return repaired


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
            measured = _validate_usage(_load_json(Path(record["path"]), "v194 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative_lower = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], usage
    )
    failure = {
        "schema_version": V194_FAILURE_VERSION,
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
        + unknown * MAXIMUM_TOTAL_TOKENS_PER_TURN,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V194_TERMINAL_VERSION,
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


async def run_v194(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v194 terminal")
    frozen = freeze_v194(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            patch_output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=frozen["instructions"],
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=len(frozen["packets"]),
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_metric_patch_output(
                    candidate, frozen["packets"]
                ),
            )
        repaired = _apply_patches(
            output=frozen["predecessor"]["output"],
            packets=frozen["packets"],
            patches=patch_output,
        )
        prepared = [
            {
                "segment_id": row["case_id"],
                "segment_text": row["source_excerpt"],
                "boundaries": row["boundaries"],
            }
            for row in frozen["predecessor"]["predecessor"]["private_input"]["cases"]
        ]
        normalized, diagnostics = normalize_episode_batch_output(
            repaired,
            episode_id=v192.DIAGNOSTIC_EPISODE_ID,
            prepared_segments=prepared,
            max_events_per_segment=v192.MAX_EVENTS_PER_CASE,
        )
        repaired_path = root / "residual-repair-metric-patched-output.private.json"
        diagnostics_path = root / "metric-patched-structural-diagnostics.json"
        _write_immutable(repaired_path, normalized)
        _write_immutable(diagnostics_path, {"segments": diagnostics})
        accounting = _aggregate_usage([sidecar])
        combined_repair_usage = _sum_usage(
            frozen["predecessor"]["usage"], accounting["usage"]
        )
        gate = v193._phase_one_gate(
            normalized=normalized,
            diagnostics=diagnostics,
            usage=combined_repair_usage,
            predecessor=frozen["predecessor"]["predecessor"],
        )
        gate = {
            **gate,
            "schema_version": V194_GATE_VERSION,
            "v193_repair_usage": frozen["predecessor"]["usage"],
            "v194_metric_patch_usage": accounting["usage"],
            "combined_repair_usage": combined_repair_usage,
            "metric_patch_count": len(frozen["packets"]),
            "non_metric_event_fields_changed": False,
        }
        gate_path = root / "metric-patched-phase-one-gate.json"
        _write_immutable(gate_path, gate)
        cumulative_lower = _sum_usage(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        terminal = {
            "schema_version": V194_TERMINAL_VERSION,
            "state": "completed" if gate["passed"] else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v194_metric_patch_cleared_phase_one_gates_support_judge_authorized"
                if gate["passed"]
                else "v194_metric_patch_phase_one_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "repaired_output": _record(repaired_path),
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
                / "development-selection-v5_4-v195-residual-repair-support"
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
    parser = argparse.ArgumentParser(description="Run v194 metric-only patch repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v194(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
