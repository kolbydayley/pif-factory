from __future__ import annotations

"""Adopt the completed v221 turns and stop its eliminated strategy."""

import argparse
import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v174_exact_evidence_continuation as v174
from . import app_server_judge_v5_selection_v220_integrated_base_design as v220
from . import app_server_judge_v5_selection_v221_integrated_base_canary as v221
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .paths import db_path
from .util import now_iso


V222_ADOPTION_VERSION = (
    "pif_app_server_judge_v5_4_selection_v222_partial_adoption_v1"
)
V222_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v222_gate_v1"
V222_REPORT_VERSION = "pif_app_server_judge_v5_4_selection_v222_report_v1"
V222_NEXT_VERSION = "pif_app_server_judge_v5_4_selection_v222_next_v1"
V222_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v222_spec_v1"
V222_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v222_runtime_lock_v1"
)
V222_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v222_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v222_integrated_partial_nonacceptance"
DEFAULT_OUTPUT_ROOT = (
    v220.PIPELINE_ROOT
    / "development-selection-v5_4-v222-integrated-partial-nonacceptance"
).resolve()

COMPLETED_TURN_COUNT = 2
EXPECTED_USAGE = {
    "input_tokens": 61_396,
    "cached_input_tokens": 0,
    "output_tokens": 9_205,
    "reasoning_output_tokens": 728,
    "total_tokens": 70_601,
}
EXPECTED_CUMULATIVE_KNOWN_USAGE = {
    "input_tokens": 7_738_773,
    "cached_input_tokens": 924_032,
    "output_tokens": 1_418_441,
    "reasoning_output_tokens": 460_993,
    "total_tokens": 9_157_214,
}
EXPECTED_UNKNOWN_TURN_COUNT = 3
EXPECTED_UNKNOWN_UPPER_BOUND = 245_000
DENSE_EVENT_COUNT_RATIO_MINIMUM = 0.75


class JudgeV5SelectionV222Error(RuntimeError):
    """The v221 partial attempt cannot be adopted without replay."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _error_receipt_message_matches(path: Path) -> bool:
    failure = _load_json(path, "v221 failure")
    return (
        failure.get("error_message_sha256")
        == "00d135cd2a04be8766ccb339a2ea3225b066786819c916bfd2151f7c2b5c56aa"
        and failure.get("error_message_bytes") == 45
    )


def _v221_top_paths() -> dict[str, Path]:
    root = v221.DEFAULT_OUTPUT_ROOT
    return {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "launch": root / "launch-receipt.json",
        "runtime_lock": root / "runtime-lock.json",
        "spec": root / "attempt-spec.json",
        "capacity_policy": root / "capacity-policy.json",
        "capacity_audit": root / "capacity-policy-audit.json",
    }


def _expected_v221_paths(
    turn_names: Sequence[str],
) -> dict[str, Path]:
    if len(turn_names) != v220.TURN_COUNT:
        raise JudgeV5SelectionV222Error("v221 turn count drifted")
    paths = dict(_v221_top_paths())
    for index, turn_name in enumerate(turn_names):
        turn_paths = v221._turn_artifact_paths(
            v221.DEFAULT_OUTPUT_ROOT, turn_name
        )
        for name in ("base_instructions", "prompt", "schema", "input"):
            paths[f"{turn_name}:{name}"] = turn_paths[name]
        if index < COMPLETED_TURN_COUNT:
            for name in ("capacity", "sidecar", "output"):
                paths[f"{turn_name}:{name}"] = turn_paths[name]
        if index == 0:
            for name in ("normalized", "diagnostics"):
                paths[f"{turn_name}:{name}"] = turn_paths[name]
    return paths


def _validate_v221_partial(
    *, database_path: Optional[Path] = None
) -> dict[str, Any]:
    predecessor = v221._validate_v220_authorization()
    top_paths = _v221_top_paths()
    spec = _load_json(top_paths["spec"], "v221 spec")
    turn_names = [str(value) for value in spec.get("turn_plan") or []]
    expected_order = [
        str(row["turn_name"])
        for row in predecessor["request_fingerprints"]["turns"]
    ]
    if turn_names != expected_order:
        raise JudgeV5SelectionV222Error("v221 frozen turn order drifted")
    paths = _expected_v221_paths(turn_names)
    actual = {
        path.resolve()
        for path in v221.DEFAULT_OUTPUT_ROOT.rglob("*")
        if path.is_file()
    }
    expected = {path.resolve() for path in paths.values()}
    if actual != expected:
        raise JudgeV5SelectionV222Error("v221 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV222Error("v221 immutable artifact drifted")
    v221.verify_runtime_lock(
        top_paths["runtime_lock"], predecessor=predecessor
    )

    terminal = _load_json(top_paths["terminal"], "v221 terminal")
    failure = _load_json(top_paths["failure"], "v221 failure")
    launch = _load_json(top_paths["launch"], "v221 launch")
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason")
        != "infrastructure_or_extraction_attempt_failed"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not False
        or terminal.get("usage") != EXPECTED_USAGE
        or terminal.get("cumulative_known_usage_lower_bound")
        != EXPECTED_CUMULATIVE_KNOWN_USAGE
        or terminal.get("cumulative_unknown_usage_turn_count")
        != EXPECTED_UNKNOWN_TURN_COUNT
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound")
        != EXPECTED_UNKNOWN_UPPER_BOUND
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("semantic_retry_allowed") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("attempted_turn_count") != COMPLETED_TURN_COUNT
        or failure.get("measured_turn_count") != COMPLETED_TURN_COUNT
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage_status") != "complete"
        or failure.get("usage") != EXPECTED_USAGE
        or failure.get("semantic_retry_allowed") is not False
        or launch.get("declared_turn_count") != v220.TURN_COUNT
        or launch.get("retry_count_per_turn") != 0
        or launch.get("managed_chatgpt_auth_only") is not True
        or launch.get("holdout_authorized") is not False
        or launch.get("production_mutation_allowed") is not False
        or not _error_receipt_message_matches(top_paths["failure"])
    ):
        raise JudgeV5SelectionV222Error("v221 partial terminal contract drifted")

    source_db = (database_path or db_path()).expanduser().resolve()
    conn = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        requests = v221._build_turn_requests(
            conn, predecessor=predecessor
        )
    finally:
        conn.close()
    if [str(row["turn_name"]) for row in requests] != turn_names:
        raise JudgeV5SelectionV222Error("v221 rebuilt request order drifted")

    adopted = []
    usage = {field: 0 for field in USAGE_FIELDS}
    for index, (turn_name, request) in enumerate(zip(turn_names, requests)):
        turn_paths = v221._turn_artifact_paths(
            v221.DEFAULT_OUTPUT_ROOT, turn_name
        )
        if index >= COMPLETED_TURN_COUNT:
            forbidden = (
                "capacity",
                "sidecar",
                "output",
                "normalized",
                "diagnostics",
            )
            if any(turn_paths[name].exists() for name in forbidden):
                raise JudgeV5SelectionV222Error(
                    "v221 not-started turn gained semantic artifacts"
                )
            continue
        capacity = _load_json(turn_paths["capacity"], "v221 capacity")
        sidecar = _load_json(turn_paths["sidecar"], "v221 sidecar")
        output = _load_json(turn_paths["output"], "v221 output")
        turn_usage = _validate_usage(sidecar)
        if (
            capacity.get("cleared_for_semantic_turn") is not True
            or capacity.get("managed_chatgpt_auth_verified") is not True
            or capacity.get("rate_limit_reached_type") is not None
            or sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("plan_type") != "pro"
            or sidecar.get("model") != v220.MODEL
            or sidecar.get("effort") != v220.EFFORT
            or sidecar.get("error_class") is not None
        ):
            raise JudgeV5SelectionV222Error(
                "v221 completed turn contract drifted"
            )
        for field in USAGE_FIELDS:
            usage[field] += int(turn_usage[field])
        normalized, diagnostics = v221.validate_and_normalize_output(
            payload=output, request=request
        )
        if index == 0:
            persisted_normalized = _load_json(
                turn_paths["normalized"], "v221 normalized"
            )
            persisted_diagnostics = _load_json(
                turn_paths["diagnostics"], "v221 diagnostics"
            )
            if (
                _canonical_json(normalized)
                != _canonical_json(persisted_normalized)
                or persisted_diagnostics.get("turn_name") != turn_name
                or persisted_diagnostics.get("episode_id")
                != request["episode_id"]
                or _canonical_json(diagnostics)
                != _canonical_json(persisted_diagnostics.get("segments"))
            ):
                raise JudgeV5SelectionV222Error(
                    "v221 persisted normalized output drifted"
                )
        adopted.append(
            {
                "turn_name": turn_name,
                "request": request,
                "output_record": _record(turn_paths["output"]),
                "sidecar_record": _record(turn_paths["sidecar"]),
                "capacity_record": _record(turn_paths["capacity"]),
                "usage": turn_usage,
                "normalized": normalized,
                "diagnostics": diagnostics,
            }
        )
    if usage != EXPECTED_USAGE or len(adopted) != COMPLETED_TURN_COUNT:
        raise JudgeV5SelectionV222Error("v221 adopted usage drifted")
    return {
        "root": v221.DEFAULT_OUTPUT_ROOT,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "predecessor": predecessor,
        "terminal": terminal,
        "failure": failure,
        "turn_names": turn_names,
        "completed_turn_names": turn_names[:COMPLETED_TURN_COUNT],
        "not_started_turn_names": turn_names[COMPLETED_TURN_COUNT:],
        "requests": requests,
        "adopted": adopted,
        "usage": usage,
    }


def _validate_frozen_v174_judge() -> dict[str, Any]:
    root = v174.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    terminal = _load_json(terminal_path, "v174 terminal")
    metrics = terminal.get("metrics") or {}
    required_records = {
        "terminal": _record(terminal_path),
        "protocol": terminal.get("protocol"),
        "reference": terminal.get("reference"),
        "score": terminal.get("score"),
    }
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v174_full_development_calibration_passed_selection_authorized"
        or terminal.get("development_judge_frozen") is not True
        or terminal.get("selection_authorized") is not True
        or terminal.get("failed_quality_gates") != []
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("production_mutated") is not False
        or metrics.get("case_count") != 66
        or metrics.get("witness_count") != 182
        or float(metrics.get("alignment_f1", 0.0)) < 0.95
        or float(metrics.get("field_diagnostic_f1", 0.0)) < 0.95
        or float(metrics.get("structured_field_accuracy", 0.0)) < 0.95
        or float(metrics.get("support_sensitivity", 0.0)) < 0.95
        or float(metrics.get("support_specificity", 0.0)) < 0.95
        or float(metrics.get("order_bias", 1.0)) > 0.05
        or any(
            not isinstance(record, Mapping) or not _verify_record(record)
            for record in required_records.values()
        )
    ):
        raise JudgeV5SelectionV222Error("v174 frozen judge contract drifted")
    return {
        "root": root,
        "terminal": terminal,
        "records": required_records,
    }


def _partial_gate(
    partial: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = partial["predecessor"]["manifest"]
    metadata = {
        str(segment["segment_id"]): {
            "episode_id": str(episode["episode_id"]),
            "source_id": str(episode["source_id"]),
            "density_stratum": str(segment["density_stratum"]),
            "golden_event_count": int(segment["golden_event_count"]),
        }
        for episode in manifest.get("episodes") or []
        for segment in episode.get("segments") or []
    }
    rows = []
    for turn in partial["adopted"]:
        for diagnostic in turn["diagnostics"]:
            segment_id = str(diagnostic["segment_id"])
            item = {**metadata[segment_id], **dict(diagnostic)}
            golden = int(item["golden_event_count"])
            item["candidate_to_reference_event_count_ratio"] = (
                round(int(item["output_events"]) / golden, 6)
                if golden
                else None
            )
            rows.append(item)
    if len(rows) != COMPLETED_TURN_COUNT * v220.SEGMENTS_PER_EPISODE:
        raise JudgeV5SelectionV222Error("v222 adopted segment count drifted")
    dense = [row for row in rows if row["density_stratum"] == "dense"]
    no_signal = [
        row for row in rows if row["density_stratum"] == "no_signal"
    ]
    if len(dense) != 2 or len(no_signal) != 2:
        raise JudgeV5SelectionV222Error("v222 density coverage drifted")
    shortfalls = [
        row
        for row in dense
        if float(row["candidate_to_reference_event_count_ratio"])
        < DENSE_EVENT_COUNT_RATIO_MINIMUM
    ]
    unflagged = [
        row for row in shortfalls if row["coverage_status"] == "complete"
    ]
    no_signal_positive = [
        row for row in no_signal if int(row["output_events"]) > 0
    ]
    total_input = sum(int(row["input_events"]) for row in rows)
    total_output = sum(int(row["output_events"]) for row in rows)
    exact_rate = round(total_output / total_input, 6) if total_input else 1.0
    observed_segments = len(rows)
    scaled_tokens = math.ceil(
        int(partial["usage"]["total_tokens"])
        * v220.BASELINE_SEGMENT_SCOPE
        / observed_segments
    )
    projected_total = scaled_tokens + v220.PRODUCTION_AMORTIZED_CONTEXT_TOKENS
    projected_ratio = round(
        projected_total / v220.BASELINE_END_TO_END_TOKENS, 6
    )
    ratios = sorted(
        float(row["candidate_to_reference_event_count_ratio"])
        for row in dense
    )
    checks = {
        "completed_outputs_validated": len(partial["adopted"])
        == COMPLETED_TURN_COUNT,
        "completed_usage_measured": partial["usage"] == EXPECTED_USAGE,
        "normalized_exact_evidence_rate_1": exact_rate == 1.0,
        "metric_grounding_error_events_0": all(
            int(row["metric_grounding_error_events"]) == 0 for row in rows
        ),
        "coverage_receipt_contract_valid": all(
            row["coverage_receipt_valid"] is True for row in rows
        ),
        "observed_projection_token_ratio_lte_0_18": projected_ratio <= 0.18,
        "unflagged_dense_coverage_shortfall_count_0": not unflagged,
    }
    gate = {
        "schema_version": V222_GATE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(
            name for name, passed in checks.items() if not passed
        ),
        "completed_turn_count": COMPLETED_TURN_COUNT,
        "not_started_turn_count": v220.TURN_COUNT - COMPLETED_TURN_COUNT,
        "validated_segment_count": len(rows),
        "dense_segment_count": len(dense),
        "no_signal_segment_count": len(no_signal),
        "dense_candidate_to_reference_event_count_ratios": ratios,
        "dense_coverage_shortfall_count": len(shortfalls),
        "unflagged_dense_coverage_shortfall_count": len(unflagged),
        "no_signal_candidate_positive_segment_count": len(no_signal_positive),
        "no_signal_candidate_positive_disposition": (
            "requires_frozen_llm_adjudication_not_automatically_false_positive"
        ),
        "normalized_exact_evidence_rate": exact_rate,
        "observed_production_amortized_total_tokens": projected_total,
        "observed_production_amortized_total_token_ratio": projected_ratio,
        "observed_projection_token_gate_passed": projected_ratio <= 0.18,
        "full_strategy_token_target_measured": False,
        "remaining_turns_can_change_frozen_v220_strategy_verdict": False,
        "reason": (
            "the frozen v220 gate requires zero unflagged dense shortfalls; "
            "two are already observed and later turns cannot remove them"
        ),
        "fresh_frozen_judge_audit_authorized": True,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }
    private = {
        "schema_version": V222_ADOPTION_VERSION,
        "turns": [
            {
                "turn_name": turn["turn_name"],
                "output_record": turn["output_record"],
                "sidecar_record": turn["sidecar_record"],
                "capacity_record": turn["capacity_record"],
                "usage": turn["usage"],
                "normalized": turn["normalized"],
                "diagnostics": turn["diagnostics"],
            }
            for turn in partial["adopted"]
        ],
        "cases": rows,
        "privacy": "private_ids_events_and_diagnostics_no_supervisor_text",
    }
    return gate, private


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v221._expected_runtime_paths())
            | {
                Path(__file__).resolve(),
                Path(v174.__file__).resolve(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v222 runtime lock")
    expected_runtime = {str(item) for item in _expected_runtime_paths()}
    actual_runtime = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping) and isinstance(record.get("path"), str)
    }
    if (
        lock.get("schema_version") != V222_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or actual_runtime != expected_runtime
        or lock.get("semantic_model_calls_allowed") is not False
        or lock.get("extraction_replay_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV222Error("v222 runtime lock drifted")
    records = [
        *(lock.get("runtime_files") or []),
        *(lock.get("v221_attempt") or []),
        *(lock.get("frozen_judge") or []),
        lock.get("spec"),
        *(lock.get("outputs") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV222Error("v222 runtime lock record drifted")
    return lock


def freeze_v222(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Optional[Path] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v222 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV222Error(
            "v222 root is nonempty without a terminal"
        )
    partial = _validate_v221_partial(database_path=database_path)
    frozen_judge = _validate_frozen_v174_judge()
    gate, private = _partial_gate(partial)
    if (
        gate.get("passed") is not False
        or gate.get("failed_checks")
        != ["unflagged_dense_coverage_shortfall_count_0"]
        or gate.get("dense_candidate_to_reference_event_count_ratios")
        != [0.407407, 0.73913]
        or gate.get("unflagged_dense_coverage_shortfall_count") != 2
        or gate.get("remaining_turns_can_change_frozen_v220_strategy_verdict")
        is not False
    ):
        raise JudgeV5SelectionV222Error("v222 partial gate drifted")

    private_path = root / "partial-adoption.private.json"
    gate_path = root / "partial-structural-gate.json"
    _write_immutable(private_path, private)
    _write_immutable(gate_path, gate)
    report = {
        "schema_version": V222_REPORT_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "inactive_incomplete_recovery_required",
        "blocker_class": "integrated_completeness_routing_quality_failure",
        "predecessor_failure_class": (
            "local_frozen_turn_token_bound_exceeded_after_measured_completion"
        ),
        "completed_turn_count": COMPLETED_TURN_COUNT,
        "not_started_turn_count": v220.TURN_COUNT - COMPLETED_TURN_COUNT,
        "not_started_turns_cancelled_without_semantic_attempt": True,
        "dense_candidate_to_reference_event_count_ratios": gate[
            "dense_candidate_to_reference_event_count_ratios"
        ],
        "unflagged_dense_coverage_shortfall_count": gate[
            "unflagged_dense_coverage_shortfall_count"
        ],
        "no_signal_candidate_positive_segment_count": gate[
            "no_signal_candidate_positive_segment_count"
        ],
        "no_signal_candidate_positive_disposition": gate[
            "no_signal_candidate_positive_disposition"
        ],
        "observed_production_amortized_total_token_ratio": gate[
            "observed_production_amortized_total_token_ratio"
        ],
        "observed_projection_token_gate_passed": gate[
            "observed_projection_token_gate_passed"
        ],
        "semantic_quality_passed": False,
        "more_development_turns_can_change_current_strategy_verdict": False,
        "viable_systems_meeting_joint_gates": [],
        "unresolved_selection_decision": (
            "whether apparent candidate omissions are genuine source-supported "
            "events or noisy one-sided reference records"
        ),
        "cumulative_known_total_tokens": EXPECTED_CUMULATIVE_KNOWN_USAGE[
            "total_tokens"
        ],
        "cumulative_unknown_usage_turn_count": EXPECTED_UNKNOWN_TURN_COUNT,
        "cumulative_unknown_usage_upper_bound_tokens": (
            EXPECTED_UNKNOWN_UPPER_BOUND
        ),
        "shortest_path_to_holdout_verdict": (
            "use the already-frozen v174 judge only on observed dense omissions "
            "and the one no-signal candidate-only event; do not replay extraction"
        ),
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "sanitized_counts_hashes_and_metrics_no_source_or_event_text",
    }
    report_path = root / "development-nonacceptance-report.json"
    _write_stable_time(report_path, report, "created_at")
    next_experiment = {
        "schema_version": V222_NEXT_VERSION,
        "state": "authorized_not_started",
        "strategy": "frozen_v174_side_free_observed_residual_audit",
        "semantic_scope": {
            "dense_apparent_omission_groups": 2,
            "no_signal_candidate_only_event_groups": 1,
            "source_side_free": True,
            "judge_protocol_changed": False,
            "judge_rubric_changed": False,
            "extraction_replay": False,
        },
        "decision": (
            "classify observed one-sided residuals as supported event, duplicate, "
            "unsupported record, or abstain using the frozen judge"
        ),
        "promotion_rule": (
            "only source-supported distinct omissions count against extraction; "
            "one-sided candidate events are never deterministically false positives"
        ),
        "stop_rule": (
            "stop after the bounded residual audit and freeze its measured usage; "
            "do not run either untouched v221 extraction turn"
        ),
        "frozen_judge": frozen_judge["records"],
        "holdout_remains_closed": True,
        "production_mutation_allowed": False,
    }
    next_path = root / "next-experiment.json"
    _write_immutable(next_path, next_experiment)
    spec = {
        "schema_version": V222_SPEC_VERSION,
        "state": "zero_model_call_partial_nonacceptance_completed",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "v221_completed_outputs_adopted_without_replay": True,
        "v221_terminal_mutated": False,
        "v221_not_started_turns_replayed": False,
        "semantic_model_calls_started": 0,
        "extraction_model_calls_started": 0,
        "new_usage_tokens": 0,
        "fresh_frozen_judge_audit_authorized": True,
        "development_winner_frozen": False,
        "holdout_started": False,
        "production_mutation_allowed": False,
        "v221_attempt": partial["records"],
        "frozen_judge": frozen_judge["records"],
        "outputs": {
            "private_adoption": _record(private_path),
            "gate": _record(gate_path),
            "report": _record(report_path),
            "next_experiment": _record(next_path),
        },
        "privacy": "private_adoption_separate_from_sanitized_reports",
    }
    spec_path = root / "nonacceptance-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    runtime_lock = {
        "schema_version": V222_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "runtime_files": [_record(path) for path in _expected_runtime_paths()],
        "v221_attempt": list(partial["records"].values()),
        "frozen_judge": list(frozen_judge["records"].values()),
        "spec": _record(spec_path),
        "outputs": [
            _record(private_path),
            _record(gate_path),
            _record(report_path),
            _record(next_path),
        ],
        "semantic_model_calls_allowed": False,
        "extraction_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    lock_path = root / "runtime-lock.json"
    _write_stable_time(lock_path, runtime_lock, "created_at")
    verify_runtime_lock(lock_path)
    terminal = {
        "schema_version": V222_TERMINAL_VERSION,
        "state": "development_strategy_not_accepted",
        "terminal_at": now_iso(),
        "terminal_reason": (
            "v222_integrated_completeness_routing_gate_not_passed_remaining_"
            "turns_cancelled"
        ),
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "evaluation_accepted": False,
        "semantic_quality_passed": False,
        "full_strategy_token_target_measured": False,
        "observed_projection_token_gate_passed": gate[
            "observed_projection_token_gate_passed"
        ],
        "development_winner_frozen": False,
        "fresh_frozen_judge_audit_authorized": True,
        "v221_completed_turn_count": COMPLETED_TURN_COUNT,
        "v221_not_started_turn_count": v220.TURN_COUNT
        - COMPLETED_TURN_COUNT,
        "v221_not_started_turns_replayed": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_known_usage_lower_bound": (
            EXPECTED_CUMULATIVE_KNOWN_USAGE
        ),
        "cumulative_unknown_usage_turn_count": EXPECTED_UNKNOWN_TURN_COUNT,
        "cumulative_conservative_unknown_usage_upper_bound": (
            EXPECTED_UNKNOWN_UPPER_BOUND
        ),
        "report": _record(report_path),
        "gate": _record(gate_path),
        "next_experiment": _record(next_path),
        "spec": _record(spec_path),
        "runtime_lock": _record(lock_path),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v223-frozen-judge-residual-audit"
            / "terminal.json"
        ),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Freeze v222 partial integrated nonacceptance"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--database-path")
    args = parser.parse_args(argv)
    terminal = freeze_v222(
        output_dir=Path(args.output_dir),
        database_path=(Path(args.database_path) if args.database_path else None),
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "evaluation_accepted": terminal["evaluation_accepted"],
                "fresh_frozen_judge_audit_authorized": terminal[
                    "fresh_frozen_judge_audit_authorized"
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
