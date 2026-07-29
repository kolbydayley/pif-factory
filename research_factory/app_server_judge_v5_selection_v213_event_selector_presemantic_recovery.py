from __future__ import annotations

"""Recover the v212 freeze-only/run handoff without replaying a semantic turn."""

import argparse
import asyncio
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity_module
from . import app_server_capacity_reserve as reserve_module
from . import app_server_dev_selection as selection_module
from . import app_server_judge_v5_calibration_v26_diagnostic as v26
from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v201_residual_repair_score as v201
from . import app_server_judge_v5_selection_v207_adaptive_router_design as v207
from . import app_server_judge_v5_selection_v208_adaptive_router_canary as v208
from . import app_server_judge_v5_selection_v209_adaptive_router_schema_recovery as v209
from . import app_server_judge_v5_selection_v210_adaptive_router_nonacceptance as v210
from . import app_server_judge_v5_selection_v211_event_selector_design as v211
from . import app_server_judge_v5_selection_v212_event_selector_canary as v212
from . import app_server_llm_judge as llm_judge_module
from . import codex_app_server as codex_app_server_module
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import load_reserve_capacity_policy
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
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
from .util import now_iso


V213_INCIDENT_VERSION = "pif_app_server_judge_v5_4_selection_v213_incident_v1"
V213_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V213_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V213_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v213_spec_v1"
V213_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v213_runtime_lock_v1"
V213_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v213_launch_v1"
V213_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v213_failure_v1"
V213_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v213_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v213_event_selector_presemantic_recovery"
TURN_NAME = "selection_event_selector_presemantic_recovery_00"
DEFAULT_OUTPUT_ROOT = (
    v212.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v213-event-selector-presemantic-recovery"
).resolve()


class JudgeV5SelectionV213Error(RuntimeError):
    """The v212 presemantic recovery cannot continue safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _v212_presemantic_paths(root: Path = v212.DEFAULT_OUTPUT_ROOT) -> dict[str, Path]:
    turn_root = root / "turns" / v212.TURN_NAME.replace("_", "-")
    return {
        "runtime_lock": root / "runtime-lock.json",
        "spec": root / "attempt-spec.json",
        "capacity_policy": root / "capacity-policy.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
    }


def _validate_v212_presemantic_failure() -> dict[str, Any]:
    root = v212.DEFAULT_OUTPUT_ROOT
    paths = _v212_presemantic_paths(root)
    expected_files = {path.resolve() for path in paths.values()}
    actual_files = {path.resolve() for path in root.rglob("*") if path.is_file()}
    forbidden = [
        root / "terminal.json",
        root / "failure.json",
        root / "launch-receipt.json",
        root / "turns" / v212.TURN_NAME.replace("_", "-") / "capacity.json",
        root / "turns" / v212.TURN_NAME.replace("_", "-") / "sidecar.json",
        root / "turns" / v212.TURN_NAME.replace("_", "-") / "output.private.json",
    ]
    if actual_files != expected_files or any(path.exists() for path in forbidden):
        raise JudgeV5SelectionV213Error("v212 presemantic absence contract drifted")
    values = {
        name: _load_json(path, f"v212 presemantic {name}")
        for name, path in paths.items()
        if name != "prompt"
    }
    prompt = paths["prompt"].read_text(encoding="utf-8")
    spec = values["spec"]
    if (
        spec.get("state") != "frozen_before_model_call"
        or spec.get("turn_plan") != [v212.TURN_NAME]
        or spec.get("retry_count_per_turn") != 0
        or spec.get("extraction_model_calls_authorized") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
        or spec.get("frozen_request", {}).get("input") != _record(paths["input"])
        or spec.get("frozen_request", {}).get("prompt") != _record(paths["prompt"])
        or spec.get("frozen_request", {}).get("schema") != _record(paths["schema"])
    ):
        raise JudgeV5SelectionV213Error("v212 presemantic spec drifted")
    v212.verify_runtime_lock(paths["runtime_lock"])
    for path in paths.values():
        if not _verify_record(_record(path)):
            raise JudgeV5SelectionV213Error("v212 presemantic artifact drifted")
    predecessor = v212._validate_v211_authorization()
    if (
        values["input"] != predecessor["input"]
        or values["schema"] != predecessor["schema"]
        or prompt != predecessor["prompt"]
    ):
        raise JudgeV5SelectionV213Error("v212 frozen request drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "spec": spec,
        "input": values["input"],
        "schema": values["schema"],
        "prompt": prompt,
        "predecessor": predecessor,
    }


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = v211.CANARY_SELECTOR_RUNTIME_TOKEN_BOUND
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V213_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v212_presemantic_runtime_lock": predecessor["records"]["runtime_lock"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor["predecessor"][
                "terminal"
            ]["cumulative_known_usage_lower_bound"]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["predecessor"][
                "terminal"
            ]["cumulative_unknown_usage_turn_count"],
            "predecessor_unknown_usage_upper_bound": predecessor["predecessor"][
                "terminal"
            ]["cumulative_conservative_unknown_usage_upper_bound"],
            "v212_presemantic_new_usage_tokens": 0,
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": bound,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V213_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
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
    load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(v212.__file__).resolve(),
                Path(v211.__file__).resolve(),
                Path(v210.__file__).resolve(),
                Path(v209.__file__).resolve(),
                Path(v208.__file__).resolve(),
                Path(v207.__file__).resolve(),
                Path(v201.__file__).resolve(),
                Path(v192.__file__).resolve(),
                Path(v188.__file__).resolve(),
                Path(v186.__file__).resolve(),
                Path(v26.__file__).resolve(),
                Path(reserve_module.__file__).resolve(),
                Path(capacity_module.__file__).resolve(),
                Path(codex_app_server_module.__file__).resolve(),
                Path(selection_module.__file__).resolve(),
                Path(llm_judge_module.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
            },
            key=str,
        )
    )


def _attempt_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _freeze_runtime_lock(
    *,
    root: Path,
    predecessor: Mapping[str, Any],
    incident_path: Path,
    spec_path: Path,
    capacity: Mapping[str, Path],
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V213_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "pinned_protocol_schema": _record(codex_app_server_module.PROTOCOL_SCHEMA_PATH),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v212_presemantic_attempt": list(predecessor["records"].values()),
        "v212_incident": _record(incident_path),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "managed_chatgpt_auth_only": True,
        "production_mutation_allowed": False,
    }
    _write_stable_time(path, lock, "created_at")
    verify_runtime_lock(path)
    return path


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v213 runtime lock")
    predecessor = _validate_v212_presemantic_failure()
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V213_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v212_presemantic_attempt")
        != list(predecessor["records"].values())
    ):
        raise JudgeV5SelectionV213Error("v213 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        lock.get("pinned_protocol_schema"),
        lock.get("v212_incident"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v212_presemantic_attempt") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV213Error("v213 runtime lock record drifted")
    policy = load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if policy.get("audit") != lock.get("capacity_audit"):
        raise JudgeV5SelectionV213Error("v213 policy/audit cross-link drifted")
    return lock


def _load_frozen_v213(root: Path) -> dict[str, Any]:
    semantic_paths = _attempt_paths(root)
    forbidden = [
        root / "launch-receipt.json",
        semantic_paths["capacity"],
        semantic_paths["sidecar"],
        semantic_paths["output"],
    ]
    if any(path.exists() for path in forbidden):
        raise JudgeV5SelectionV213Error("v213 started attempt cannot be silently resumed")
    required = {
        "incident": root / "v212-presemantic-incident.json",
        "spec": root / "attempt-spec.json",
        "capacity_policy": root / "capacity-policy.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "runtime_lock": root / "runtime-lock.json",
        "input": semantic_paths["input"],
        "prompt": semantic_paths["prompt"],
        "schema": semantic_paths["schema"],
    }
    if not all(path.is_file() for path in required.values()):
        raise JudgeV5SelectionV213Error("v213 frozen root is incomplete")
    verify_runtime_lock(required["runtime_lock"])
    predecessor = _validate_v212_presemantic_failure()
    spec = _load_json(required["spec"], "v213 attempt spec")
    if (
        spec.get("state") != "frozen_before_model_call"
        or spec.get("frozen_request", {}).get("input") != _record(required["input"])
        or spec.get("frozen_request", {}).get("prompt") != _record(required["prompt"])
        or spec.get("frozen_request", {}).get("schema") != _record(required["schema"])
    ):
        raise JudgeV5SelectionV213Error("v213 frozen spec drifted")
    return {
        "root": root,
        "presemantic": predecessor,
        "predecessor": predecessor["predecessor"],
        "paths": semantic_paths,
        "capacity_policy": required["capacity_policy"],
        "capacity_audit": required["capacity_audit"],
        "spec": spec,
        "spec_path": required["spec"],
        "incident": required["incident"],
        "runtime_lock": required["runtime_lock"],
    }


def freeze_v213(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = v211.TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v213 terminal")}
    runtime_lock = root / "runtime-lock.json"
    if runtime_lock.exists():
        return _load_frozen_v213(root)
    if any(root.iterdir()):
        raise JudgeV5SelectionV213Error("v213 root is partial before runtime lock")

    presemantic = _validate_v212_presemantic_failure()
    predecessor = presemantic["predecessor"]
    incident = {
        "schema_version": V213_INCIDENT_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "classification": "presemantic_local_orchestration_failure",
        "error_class": "JudgeV5SelectionV212Error",
        "error_location": "freeze_v212_rejected_previously_frozen_root",
        "semantic_attempt_started": False,
        "launch_receipt_created": False,
        "capacity_probe_started": False,
        "thread_started": False,
        "turn_started": False,
        "usage_status": "complete",
        "usage": {field: 0 for field in USAGE_FIELDS},
        "retry_of_semantic_turn": False,
        "v212_files": presemantic["records"],
        "v212_artifacts_mutated": False,
        "production_mutated": False,
    }
    incident_path = root / "v212-presemantic-incident.json"
    _write_stable_time(incident_path, incident, "created_at")
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=predecessor["input"],
        prompt=predecessor["prompt"],
        schema=predecessor["schema"],
    )
    capacity = _build_capacity_policy(root, presemantic)
    spec = {
        "schema_version": V213_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "recovery_scope": "idempotent_freeze_only_to_run_handoff_only",
        "model": v211.MODEL,
        "reasoning_effort": v211.EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "maximum_total_tokens_per_turn": v211.CANARY_SELECTOR_RUNTIME_TOKEN_BOUND,
        "promotion_total_token_gate": v211.CANARY_SELECTOR_TOTAL_TOKEN_GATE,
        "v212_semantic_turn_replayed": False,
        "v212_semantic_attempt_started": False,
        "request_semantic_contract_changed": False,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "v212_incident": _record(incident_path),
        "v212_presemantic_attempt": presemantic["records"],
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "frozen_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
            "instructions": predecessor["records"]["instructions"],
        },
        "privacy": "private_source_prompts_outputs_sanitized_counts_hashes_metrics_only",
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock_path = _freeze_runtime_lock(
        root=root,
        predecessor=presemantic,
        incident_path=incident_path,
        spec_path=spec_path,
        capacity=capacity,
    )
    return {
        "root": root,
        "presemantic": presemantic,
        "predecessor": predecessor,
        "paths": paths,
        "capacity_policy": capacity["policy"],
        "capacity_audit": capacity["audit"],
        "spec": spec,
        "spec_path": spec_path,
        "incident": incident_path,
        "runtime_lock": lock_path,
    }


def _freeze_launch_receipt(frozen: Mapping[str, Any]) -> Path:
    path = frozen["root"] / "launch-receipt.json"
    if path.exists():
        raise JudgeV5SelectionV213Error("v213 launch receipt already exists")
    receipt = {
        "schema_version": V213_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "state": "semantic_attempt_not_started",
        "declared_turn_count": 1,
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "v212_semantic_attempt_started": False,
        "capacity_checkpoint_exists_before_launch": frozen["paths"][
            "capacity"
        ].exists(),
        "sidecar_exists_before_launch": frozen["paths"]["sidecar"].exists(),
        "output_exists_before_launch": frozen["paths"]["output"].exists(),
        "production_mutated": False,
    }
    if any(
        receipt[key]
        for key in (
            "capacity_checkpoint_exists_before_launch",
            "sidecar_exists_before_launch",
            "output_exists_before_launch",
        )
    ):
        raise JudgeV5SelectionV213Error("v213 semantic artifacts predate launch")
    _write_stable_time(path, receipt, "created_at")
    return path


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], error_class: str
) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(
                _load_json(Path(sidecar_record["path"]), "v213 failed sidecar")
            )
        except Exception:
            unknown += 1
            continue
        known = _sum_usage(known, usage)
    complete = unknown == 0
    cumulative = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], known
    )
    failure = {
        "schema_version": V213_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V213_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "full_development_router_authorized": False,
        "full_development_selector_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
        "cumulative_unknown_usage_turn_count": predecessor["terminal"][
            "cumulative_unknown_usage_turn_count"
        ]
        + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": predecessor["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ]
        + unknown * v211.CANARY_SELECTOR_RUNTIME_TOKEN_BOUND,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v213(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = v211.TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v213 terminal")
    frozen = freeze_v213(output_dir=root, timeout_seconds=timeout_seconds)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = _freeze_launch_receipt(frozen)
    predecessor = frozen["predecessor"]
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=predecessor["prompt"],
                schema=predecessor["schema"],
                base_instructions=predecessor["instructions"],
                model=v211.MODEL,
                effort=v211.EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=4,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: v212.validate_selector_output(
                    value, predecessor
                ),
            )
        accounting = _aggregate_usage([sidecar])
        gate, private_score = v212._score_selector(
            output=output,
            usage=accounting["usage"],
            predecessor=predecessor,
        )
        gate_path = root / "event-selector-canary-gate.json"
        score_path = root / "event-selector-canary-score.private.json"
        _write_immutable(gate_path, gate)
        _write_immutable(score_path, private_score)
        cumulative = _sum_usage(
            predecessor["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": V213_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v213_event_selector_canary_passed_full_router_authorized"
                if passed
                else "v213_event_selector_canary_quality_or_cost_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "v212_semantic_attempt_started": False,
            "v212_semantic_turn_replayed": False,
            "extraction_model_calls_started": 0,
            "launch_receipt": _record(launch_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "gate": _record(gate_path),
            "private_score": _record(score_path),
            "full_development_router_authorized": passed,
            "full_development_selector_authorized": False,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": predecessor[
                "terminal"
            ]["cumulative_conservative_unknown_usage_upper_bound"],
            "required_next_artifact_path": (
                str(
                    root.parent
                    / "development-selection-v5_4-v214-full-coverage-router"
                    / "terminal.json"
                )
                if passed
                else None
            ),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, predecessor, exc.error_class)
    except Exception as exc:
        return _write_failure(root, predecessor, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Recover and run the v212 event-selector canary"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=v211.TIMEOUT_SECONDS)
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args(argv)
    if args.freeze_only:
        frozen = freeze_v213(
            output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds
        )
        result = {
            "state": frozen["spec"]["state"],
            "runtime_lock": str(frozen["runtime_lock"]),
            "v212_semantic_attempt_started": False,
            "holdout_authorized": False,
            "production_mutated": False,
        }
    else:
        terminal = asyncio.run(
            run_v213(
                output_dir=Path(args.output_dir),
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "full_development_router_authorized": terminal.get(
                "full_development_router_authorized", False
            ),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
            "usage_status": terminal.get("usage_status"),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
