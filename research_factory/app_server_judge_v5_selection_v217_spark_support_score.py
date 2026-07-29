from __future__ import annotations

"""Score the completed v216 Spark turn without replaying semantic work."""

import argparse
import copy
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v216_spark_support_canary as v216
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS
from .util import now_iso


V217_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v217_spec_v1"
V217_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v217_audit_v1"
V217_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v217_gate_v1"
V217_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v217_runtime_lock_v1"
)
V217_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v217_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v217_spark_support_score"
DEFAULT_OUTPUT_ROOT = (
    v216.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v217-spark-support-score-recovery"
).resolve()


class JudgeV5SelectionV217Error(RuntimeError):
    """The v217 score cannot preserve the completed v216 attempt."""


def _v216_paths() -> dict[str, Path]:
    root = v216.DEFAULT_OUTPUT_ROOT
    turn = root / "turns" / v216.TURN_NAME.replace("_", "-")
    return {
        "spec": root / "attempt-spec.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "projection_audit": root / "dictionary-projection-audit.private.json",
        "model_audit": root / "model-selection-audit.json",
        "instructions": root / "selector-instructions.private.md",
        "runtime_lock": root / "runtime-lock.json",
        "launch": root / "launch-receipt.json",
        "failure": root / "failure.json",
        "terminal": root / "terminal.json",
        "capacity": turn / "capacity.json",
        "input": turn / "input.private.json",
        "prompt": turn / "prompt.private.md",
        "schema": turn / "schema.json",
        "sidecar": turn / "sidecar.json",
        "output": turn / "output.private.json",
    }


def _validate_v216_checkpoint() -> dict[str, Any]:
    root = v216.DEFAULT_OUTPUT_ROOT
    paths = _v216_paths()
    expected = {path.resolve() for path in paths.values()}
    actual = {path.resolve() for path in root.rglob("*") if path.is_file()}
    if actual != expected:
        raise JudgeV5SelectionV217Error("v216 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV217Error("v216 immutable artifact drifted")
    v216.verify_runtime_lock(paths["runtime_lock"])

    terminal = _load_json(paths["terminal"], "v216 terminal")
    failure = _load_json(paths["failure"], "v216 failure")
    capacity = _load_json(paths["capacity"], "v216 capacity")
    sidecar = _load_json(paths["sidecar"], "v216 sidecar")
    output = _load_json(paths["output"], "v216 output")
    checkpoint = v216._validate_v215_checkpoint()
    request = v216._build_request(checkpoint["predecessor"])
    schema_errors = v216.validate_support_output(
        output,
        request["packet"],
        request["schema"],
    )
    projected = v216._project_support_output(output)
    projection_errors = v216.v212.validate_selector_output(
        projected,
        request["scoring_predecessor"],
    )
    verdicts = Counter(
        str(decision.get("verdict"))
        for case in output.get("cases") or []
        for decision in case.get("decisions") or []
    )
    expected_usage = {
        "input_tokens": 33_455,
        "cached_input_tokens": 2_176,
        "output_tokens": 8_044,
        "reasoning_output_tokens": 7_054,
        "total_tokens": 41_499,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason")
        != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != expected_usage
        or terminal.get("cumulative_known_usage_lower_bound", {}).get(
            "total_tokens"
        )
        != 9_053_190
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("usage") != expected_usage
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != v216.MODEL
        or sidecar.get("effort") != v216.EFFORT
        or sidecar.get("usage_complete") is not True
        or sidecar.get("usage") != expected_usage
        or schema_errors
        or projection_errors
        or verdicts != Counter({"s": 77, "u": 2})
    ):
        raise JudgeV5SelectionV217Error("v216 score recovery contract drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "failure": failure,
        "capacity": capacity,
        "sidecar": sidecar,
        "output": output,
        "projected": projected,
        "request": request,
    }


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v216._expected_runtime_paths())
            | {Path(v216.__file__).resolve(), Path(__file__).resolve()},
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    checkpoint: Mapping[str, Any],
    spec_path: Path,
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V217_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v216_attempt": list(checkpoint["records"].values()),
        "attempt_spec": _record(spec_path),
        "semantic_model_calls_authorized": 0,
        "production_mutation_allowed": False,
    }
    _write_immutable(path, lock)
    verify_runtime_lock(path)
    return path


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v217 runtime lock")
    checkpoint = _validate_v216_checkpoint()
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V217_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("semantic_model_calls_authorized") != 0
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v216_attempt") != list(checkpoint["records"].values())
    ):
        raise JudgeV5SelectionV217Error("v217 runtime lock drifted")
    records = [
        lock.get("attempt_spec"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v216_attempt") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV217Error("v217 runtime lock record drifted")
    return lock


def freeze_v217(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v217 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV217Error(
            "v217 root is nonempty without a terminal"
        )

    checkpoint = _validate_v216_checkpoint()
    spec = {
        "schema_version": V217_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "scope": "score_completed_v216_output_without_semantic_replay",
        "v216_turn_completed": True,
        "v216_post_turn_reserve_bound_exceeded": True,
        "v216_output_schema_and_coverage_valid": True,
        "semantic_verdict_changes_allowed": False,
        "semantic_model_calls_authorized": 0,
        "semantic_retry_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "v216_attempt": checkpoint["records"],
    }
    spec_path = root / "attempt-spec.json"
    _write_immutable(spec_path, spec)
    runtime_lock = _freeze_runtime_lock(
        root=root,
        checkpoint=checkpoint,
        spec_path=spec_path,
    )

    projected = copy.deepcopy(checkpoint["projected"])
    output_path = root / "support-positive-selector-output.private.json"
    _write_immutable(output_path, projected)
    audit = {
        "schema_version": V217_AUDIT_VERSION,
        "v216_completed_semantic_output_reused": True,
        "v216_semantic_turn_replayed": False,
        "v216_semantic_verdicts_changed": False,
        "projection_rule": "s_to_keep_self_u_to_unsupported_a_to_abstain",
        "semantic_deduplication_performed": False,
        "schema_order_and_coverage_errors": [],
        "new_semantic_model_calls": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
        "production_mutated": False,
    }
    audit_path = root / "score-recovery-audit.json"
    _write_immutable(audit_path, audit)

    gate, private_score = v216.v212._score_selector(
        output=projected,
        usage=checkpoint["sidecar"]["usage"],
        predecessor=checkpoint["request"]["scoring_predecessor"],
    )
    semantic_checks = {
        key: value
        for key, value in gate["checks"].items()
        if key != "actual_selector_total_tokens_lte_35000"
    }
    gate = {
        **gate,
        "schema_version": V217_GATE_VERSION,
        "semantic_quality_gate_passed": all(semantic_checks.values()),
        "selector_cost_gate_passed": gate["checks"][
            "actual_selector_total_tokens_lte_35000"
        ],
        "v216_semantic_turn_replayed": False,
        "new_semantic_model_calls": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
    }
    private_score = {**private_score, "gate": gate}
    gate_path = root / "spark-support-score-gate.json"
    score_path = root / "spark-support-score.private.json"
    _write_immutable(gate_path, gate)
    _write_immutable(score_path, private_score)

    if (
        gate.get("passed") is not False
        or gate.get("failed_checks")
        != ["actual_selector_total_tokens_lte_35000"]
        or gate.get("semantic_quality_gate_passed") is not True
        or gate.get("selector_cost_gate_passed") is not False
        or gate.get("candidate_mean_f1") != 0.803978
        or gate.get("mean_f1_regret_to_oracle") != 0.028563
        or gate.get("maximum_dense_case_regret_to_oracle") != 0.124286
        or gate.get("dense_improvement_count") != 4
        or gate.get("normalized_nonexact_evidence_events") != 0
        or gate.get("actual_selector_usage", {}).get("total_tokens") != 41_499
    ):
        raise JudgeV5SelectionV217Error("v217 recovered score drifted")

    terminal = {
        "schema_version": V217_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "v217_semantic_quality_passed_selector_cost_not_passed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "runtime_lock": _record(runtime_lock),
        "attempt_spec": _record(spec_path),
        "score_recovery_audit": _record(audit_path),
        "support_positive_output": _record(output_path),
        "gate": _record(gate_path),
        "private_score": _record(score_path),
        "semantic_quality_gate_passed": True,
        "selector_cost_gate_passed": False,
        "new_semantic_model_calls": 0,
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "full_development_router_authorized": False,
        "full_development_selector_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
        "cumulative_known_usage_lower_bound": checkpoint["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": checkpoint["terminal"][
            "cumulative_unknown_usage_turn_count"
        ],
        "cumulative_conservative_unknown_usage_upper_bound": checkpoint[
            "terminal"
        ]["cumulative_conservative_unknown_usage_upper_bound"],
        "viable_systems": [
            "spark_low_support_only_existing_event_selector_quality_viable"
        ],
        "unresolved_selection_decision": (
            "reduce_selector_tokens_by_at_least_6499_without_quality_regression"
        ),
        "more_development_cases_can_change_selection": False,
        "shortest_path_to_holdout_verdict": (
            "one_lossless_compact_support_canary_then_full_development_freeze"
        ),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v218-compact-support-canary"
            / "terminal.json"
        ),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Score the completed v216 Spark support output"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v217(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "semantic_quality_gate_passed": terminal[
                    "semantic_quality_gate_passed"
                ],
                "selector_cost_gate_passed": terminal[
                    "selector_cost_gate_passed"
                ],
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
                "new_semantic_model_calls": terminal["new_semantic_model_calls"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
