from __future__ import annotations

"""Normalize v214 bookkeeping IDs and score its unchanged semantic verdicts."""

import argparse
import copy
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v201_residual_repair_score as v201
from . import app_server_judge_v5_selection_v211_event_selector_design as v211
from . import app_server_judge_v5_selection_v212_event_selector_canary as v212
from . import (
    app_server_judge_v5_selection_v214_truth_conditional_selector as v214,
)
from . import labels as labels_module
from . import util as util_module
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


V215_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v215_spec_v1"
V215_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v215_audit_v1"
V215_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v215_gate_v1"
V215_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v215_runtime_lock_v1"
V215_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v215_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v215_id_normalization_score"
DEFAULT_OUTPUT_ROOT = (
    v214.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v215-selector-id-normalization-score"
).resolve()


class JudgeV5SelectionV215Error(RuntimeError):
    """The v215 deterministic normalization cannot preserve its contract."""


def _v214_paths() -> dict[str, Path]:
    root = v214.DEFAULT_OUTPUT_ROOT
    turn = root / "turns" / v214.TURN_NAME.replace("_", "-")
    return {
        "attempt_spec": root / "attempt-spec.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "defect_audit": root / "materiality-defect-audit.json",
        "projection_audit": root / "selector-projection-audit.private.json",
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


def _validate_v214_checkpoint() -> dict[str, Any]:
    root = v214.DEFAULT_OUTPUT_ROOT
    paths = _v214_paths()
    expected = {path.resolve() for path in paths.values()}
    actual = {path.resolve() for path in root.rglob("*") if path.is_file()}
    if actual != expected:
        raise JudgeV5SelectionV215Error("v214 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV215Error("v214 immutable artifact drifted")
    v214.verify_runtime_lock(paths["runtime_lock"])

    terminal = _load_json(paths["terminal"], "v214 terminal")
    failure = _load_json(paths["failure"], "v214 failure")
    sidecar = _load_json(paths["sidecar"], "v214 sidecar")
    output = _load_json(paths["output"], "v214 output")
    checkpoint = v214._validate_v213_checkpoint()
    request = v214._build_request(checkpoint["predecessor"])
    validation_errors = v212.validate_selector_output(
        output, request["scoring_predecessor"]
    )
    decisions = [
        decision
        for case in output.get("cases") or []
        for decision in case.get("decisions") or []
    ]
    verdicts = Counter(str(row.get("verdict")) for row in decisions)
    keep_link_shapes = Counter(
        "self"
        if decision.get("canonical_event_id") == decision.get("event_id")
        else "empty"
        if not decision.get("canonical_event_id")
        else "other"
        for decision in decisions
        if decision.get("verdict") == "keep"
    )
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason")
        != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 39_476
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("usage_status") != "complete"
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("usage") != terminal.get("usage")
        or validation_errors != ["keep_canonical_link"]
        or len(decisions) != 79
        or verdicts
        != Counter({"keep": 56, "duplicate": 18, "unsupported": 5})
        or keep_link_shapes != Counter({"empty": 56})
    ):
        raise JudgeV5SelectionV215Error("v214 normalization contract drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "failure": failure,
        "sidecar": sidecar,
        "output": output,
        "request": request,
    }


def _normalize_keep_links(
    output: Mapping[str, Any], scoring_predecessor: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized = copy.deepcopy(dict(output))
    changed = []
    before_verdicts = []
    for case_index, case in enumerate(normalized["cases"]):
        for decision_index, decision in enumerate(case["decisions"]):
            before_verdicts.append(
                (
                    str(case["case_id"]),
                    str(decision["event_id"]),
                    str(decision["verdict"]),
                )
            )
            if decision["verdict"] == "keep" and not decision["canonical_event_id"]:
                decision["canonical_event_id"] = decision["event_id"]
                changed.append((case_index, decision_index))
    after_verdicts = [
        (str(case["case_id"]), str(decision["event_id"]), str(decision["verdict"]))
        for case in normalized["cases"]
        for decision in case["decisions"]
    ]
    errors = v212.validate_selector_output(normalized, scoring_predecessor)
    if before_verdicts != after_verdicts or len(changed) != 56 or errors:
        raise JudgeV5SelectionV215Error("v215 ID normalization changed semantics")
    audit = {
        "schema_version": V215_AUDIT_VERSION,
        "normalization_rule": (
            "keep_with_empty_canonical_event_id_to_same_event_id_only"
        ),
        "normalized_keep_link_count": len(changed),
        "semantic_verdicts_changed": False,
        "case_order_changed": False,
        "event_order_changed": False,
        "event_payload_changed": False,
        "duplicate_links_changed": False,
        "unsupported_links_changed": False,
        "post_normalization_validation_errors": [],
        "new_semantic_model_calls": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
        "production_mutated": False,
    }
    return normalized, audit


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(v214.__file__).resolve(),
                Path(v212.__file__).resolve(),
                Path(v211.__file__).resolve(),
                Path(v201.__file__).resolve(),
                Path(v192.__file__).resolve(),
                Path(v186.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
            },
            key=str,
        )
    )


def _freeze_runtime_lock(
    *, root: Path, checkpoint: Mapping[str, Any], spec_path: Path
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V215_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v214_attempt": list(checkpoint["records"].values()),
        "attempt_spec": _record(spec_path),
        "semantic_model_calls_authorized": 0,
        "production_mutation_allowed": False,
    }
    _write_immutable(path, lock)
    verify_runtime_lock(path)
    return path


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v215 runtime lock")
    checkpoint = _validate_v214_checkpoint()
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V215_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("semantic_model_calls_authorized") != 0
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v214_attempt") != list(checkpoint["records"].values())
    ):
        raise JudgeV5SelectionV215Error("v215 runtime lock drifted")
    records = [
        lock.get("attempt_spec"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v214_attempt") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV215Error("v215 runtime lock record drifted")
    return lock


def freeze_v215(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v215 terminal")
    if any(root.iterdir()):
        raise JudgeV5SelectionV215Error("v215 root is nonempty without a terminal")

    checkpoint = _validate_v214_checkpoint()
    spec = {
        "schema_version": V215_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "scope": "deterministic_nonsemantic_id_normalization_and_scoring_only",
        "normalization_rule": (
            "keep_with_empty_canonical_event_id_to_same_event_id_only"
        ),
        "semantic_verdict_changes_allowed": False,
        "semantic_model_calls_authorized": 0,
        "semantic_retry_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "v214_attempt": checkpoint["records"],
    }
    spec_path = root / "attempt-spec.json"
    _write_immutable(spec_path, spec)
    runtime_lock = _freeze_runtime_lock(
        root=root, checkpoint=checkpoint, spec_path=spec_path
    )
    normalized, audit = _normalize_keep_links(
        checkpoint["output"], checkpoint["request"]["scoring_predecessor"]
    )
    output_path = root / "normalized-selector-output.private.json"
    audit_path = root / "id-normalization-audit.json"
    _write_immutable(output_path, normalized)
    _write_immutable(audit_path, audit)
    gate, private_score = v212._score_selector(
        output=normalized,
        usage=checkpoint["terminal"]["usage"],
        predecessor=checkpoint["request"]["scoring_predecessor"],
    )
    gate = {
        **gate,
        "schema_version": V215_GATE_VERSION,
        "semantic_verdicts_changed": False,
        "normalized_keep_link_count": 56,
        "new_semantic_model_calls": 0,
        "new_usage": {field: 0 for field in USAGE_FIELDS},
    }
    private_score = {**private_score, "gate": gate}
    gate_path = root / "normalized-selector-gate.json"
    score_path = root / "normalized-selector-score.private.json"
    _write_immutable(gate_path, gate)
    _write_immutable(score_path, private_score)
    expected_failures = {
        "actual_selector_total_tokens_lte_35000",
        "maximum_dense_case_regret_lte_0_15",
    }
    if (
        gate.get("passed") is not False
        or set(gate.get("failed_checks") or []) != expected_failures
        or gate.get("candidate_mean_f1") != 0.785931
        or gate.get("mean_f1_regret_to_oracle") != 0.04661
        or gate.get("maximum_dense_case_regret_to_oracle") != 0.250602
        or gate.get("dense_improvement_count") != 4
    ):
        raise JudgeV5SelectionV215Error("v215 normalized score drifted")
    terminal = {
        "schema_version": V215_TERMINAL_VERSION,
        "state": "inactive",
        "terminal_at": now_iso(),
        "terminal_reason": "v215_normalized_selector_quality_or_cost_gate_not_passed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "runtime_lock": _record(runtime_lock),
        "attempt_spec": _record(spec_path),
        "normalization_audit": _record(audit_path),
        "normalized_output": _record(output_path),
        "gate": _record(gate_path),
        "private_score": _record(score_path),
        "semantic_verdicts_changed": False,
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
        "cumulative_conservative_unknown_usage_upper_bound": checkpoint["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
        "viable_systems": [],
        "unresolved_selection_decision": (
            "selector_must_reduce_tokens_and_close_one_dense_case_regret"
        ),
        "more_development_cases_can_change_selection": False,
        "shortest_path_to_holdout_verdict": (
            "one_predeclared_model_and_packaging_diagnostic_then_full_canary_"
            "only_if_projected_gates_fit"
        ),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v216-model-packaging-diagnostic"
            / "terminal.json"
        ),
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Normalize and score the completed v214 selector output"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v215(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "full_development_router_authorized": terminal[
                    "full_development_router_authorized"
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
