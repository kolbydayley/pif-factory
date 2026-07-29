from __future__ import annotations

"""Recover the v208 unsupported Structured Outputs schema in one new version."""

import argparse
import asyncio
import copy
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
from . import app_server_judge_v5_selection_v206_convergence_blocker as v206
from . import app_server_judge_v5_selection_v207_adaptive_router_design as v207
from . import app_server_judge_v5_selection_v208_adaptive_router_canary as v208
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
from .app_server_llm_judge import validate_app_server_output_schema_subset
from .util import now_iso


V209_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v209_schema_audit_v1"
V209_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V209_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V209_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v209_spec_v1"
V209_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v209_runtime_lock_v1"
V209_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v209_launch_v1"
V209_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v209_failure_v1"
V209_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v209_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v209_adaptive_router_schema_recovery"
TURN_NAME = "selection_adaptive_router_schema_recovery_00"
DEFAULT_OUTPUT_ROOT = (
    v208.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v209-adaptive-router-schema-recovery"
).resolve()

EXPECTED_MESSAGE_BYTES = 353
EXPECTED_MESSAGE_SHA256 = "ee6517e02715a6459893e128051094777b39b104370f7f85a0db171e817659e5"
EXPECTED_ADDITIONAL_DETAILS_SHA256 = (
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)
EXPECTED_UNSUPPORTED_SCHEMA_PATHS = [
    "$.$schema",
    "$.properties.cases.items.properties.selected_package_ids.uniqueItems",
]


class JudgeV5SelectionV209Error(RuntimeError):
    """The one allowed v209 schema recovery cannot proceed safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v208_schema_failure() -> dict[str, Any]:
    root = v208.DEFAULT_OUTPUT_ROOT
    turn_root = root / "turns" / v208.TURN_NAME.replace("_", "-")
    output_path = turn_root / "output.private.json"
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "launch_receipt": root / "launch-receipt.json",
        "runtime_lock": root / "runtime-lock.json",
        "spec": root / "attempt-spec.json",
        "capacity_policy": root / "capacity-policy.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
    }
    values = {
        name: _load_json(path, f"v208 {name}")
        for name, path in paths.items()
        if name != "prompt"
    }
    prompt = paths["prompt"].read_text(encoding="utf-8")
    terminal = values["terminal"]
    failure = values["failure"]
    capacity = values["capacity"]
    sidecar = values["sidecar"]
    turn_error = sidecar.get("turn_error") or {}
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("usage_status") != "unknown"
        or terminal.get("accounting_complete") is not False
        or terminal.get("usage") is not None
        or terminal.get("semantic_retry_allowed") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("full_development_router_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8_900_303
        or terminal.get("cumulative_unknown_usage_turn_count") != 3
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 245_000
        or terminal.get("failure") != _record(paths["failure"])
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("failed_turn_name") != v208.TURN_NAME
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("usage_status") != "unknown"
        or failure.get("accounting_complete") is not False
        or failure.get("known_usage_lower_bound", {}).get("total_tokens") != 0
        or failure.get("unknown_usage_turn_count") != 1
        or len(failure.get("attempts") or []) != 1
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("primary_used_percent") != 8
        or capacity.get("primary_remaining_percent") != 92
        or capacity.get("rate_limit_reached_type") is not None
        or sidecar.get("state") != "failed"
        or sidecar.get("status") != "failed"
        or sidecar.get("error_class") != "turn_failed"
        or sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or sidecar.get("usage") is not None
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != v207.MODEL
        or sidecar.get("effort") != v207.EFFORT
        or turn_error.get("codex_error_info") != "other"
        or turn_error.get("message_bytes") != EXPECTED_MESSAGE_BYTES
        or turn_error.get("message_sha256") != EXPECTED_MESSAGE_SHA256
        or turn_error.get("additional_details_bytes") != 0
        or turn_error.get("additional_details_sha256")
        != EXPECTED_ADDITIONAL_DETAILS_SHA256
        or output_path.exists()
    ):
        raise JudgeV5SelectionV209Error("v208 schema-failure contract drifted")
    schema_errors = validate_app_server_output_schema_subset(values["schema"])
    if schema_errors != EXPECTED_UNSUPPORTED_SCHEMA_PATHS:
        raise JudgeV5SelectionV209Error("v208 unsupported schema paths drifted")
    for path in paths.values():
        if not _verify_record(_record(path)):
            raise JudgeV5SelectionV209Error("v208 failure artifact drifted")
    v208.verify_runtime_lock(paths["runtime_lock"])
    predecessor = v208._validate_v207_authorization()
    if prompt != predecessor["prompt"] or values["input"] != predecessor["input"]:
        raise JudgeV5SelectionV209Error("v208 request differs from v207 authorization")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "failure": failure,
        "capacity": capacity,
        "sidecar": sidecar,
        "schema": values["schema"],
        "prompt": prompt,
        "input": values["input"],
        "predecessor": predecessor,
    }


def repair_schema_v209(value: Mapping[str, Any]) -> dict[str, Any]:
    def visit(item: Any) -> Any:
        if isinstance(item, dict):
            return {
                key: visit(child)
                for key, child in item.items()
                if key not in {"$schema", "uniqueItems"}
            }
        if isinstance(item, list):
            return [visit(child) for child in item]
        return copy.deepcopy(item)

    corrected = visit(value)
    if validate_app_server_output_schema_subset(corrected):
        raise JudgeV5SelectionV209Error("v209 corrected schema remains unsupported")
    return corrected


def validate_router_output_v209(
    output: Any, predecessor: Mapping[str, Any], schema: Mapping[str, Any]
) -> list[str]:
    if isinstance(output, dict):
        for row in output.get("cases") or []:
            selected = row.get("selected_package_ids") if isinstance(row, dict) else None
            if isinstance(selected, list) and len(selected) != len(set(selected)):
                return ["duplicate_package_id"]
    projected = dict(predecessor)
    projected["schema"] = schema
    return v208.validate_router_output(output, projected)


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = v207.MAX_CANARY_TOTAL_TOKENS
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V209_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v208_terminal": predecessor["records"]["terminal"],
        "v208_failure": predecessor["records"]["failure"],
        "v208_unknown_usage_preserved": True,
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor["terminal"][
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": predecessor["terminal"][
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": bound,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V209_CAPACITY_POLICY_VERSION,
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
                Path(v208.__file__).resolve(),
                Path(v207.__file__).resolve(),
                Path(v206.__file__).resolve(),
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


def _freeze_runtime_lock(
    *,
    root: Path,
    predecessor: Mapping[str, Any],
    spec_path: Path,
    audit_path: Path,
    capacity: Mapping[str, Path],
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V209_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "pinned_protocol_schema": _record(codex_app_server_module.PROTOCOL_SCHEMA_PATH),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v208_failed_attempt": [
            predecessor["records"][name]
            for name in (
                "terminal",
                "failure",
                "launch_receipt",
                "runtime_lock",
                "spec",
                "capacity_policy",
                "capacity_audit",
                "capacity",
                "sidecar",
                "input",
                "prompt",
                "schema",
            )
        ],
        "schema_recovery_audit": _record(audit_path),
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
    lock = _load_json(path, "v209 runtime lock")
    predecessor = _validate_v208_schema_failure()
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    expected_v208 = [
        predecessor["records"][name]
        for name in (
            "terminal",
            "failure",
            "launch_receipt",
            "runtime_lock",
            "spec",
            "capacity_policy",
            "capacity_audit",
            "capacity",
            "sidecar",
            "input",
            "prompt",
            "schema",
        )
    ]
    if (
        lock.get("schema_version") != V209_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v208_failed_attempt") != expected_v208
    ):
        raise JudgeV5SelectionV209Error("v209 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        lock.get("pinned_protocol_schema"),
        lock.get("schema_recovery_audit"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v208_failed_attempt") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV209Error("v209 runtime lock record drifted")
    policy = load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if policy.get("audit") != lock.get("capacity_audit"):
        raise JudgeV5SelectionV209Error("v209 policy/audit cross-link drifted")
    return lock


def freeze_v209(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = v207.TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v209 terminal")}
    predecessor = _validate_v208_schema_failure()
    corrected_schema = repair_schema_v209(predecessor["schema"])
    schema_audit = {
        "schema_version": V209_AUDIT_VERSION,
        "created_at": now_iso(),
        "v208_schema": predecessor["records"]["schema"],
        "unsupported_paths_before": EXPECTED_UNSUPPORTED_SCHEMA_PATHS,
        "unsupported_paths_after": validate_app_server_output_schema_subset(
            corrected_schema
        ),
        "removed_keywords": ["$schema", "uniqueItems"],
        "prompt_changed": False,
        "input_changed": False,
        "model_or_effort_changed": False,
        "semantic_contract_changed": False,
        "package_uniqueness_moved_to_deterministic_post_generation_validation": True,
        "v208_unknown_usage_preserved": True,
        "production_mutated": False,
    }
    audit_path = root / "schema-recovery-audit.json"
    _write_stable_time(audit_path, schema_audit, "created_at")
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=predecessor["input"],
        prompt=predecessor["prompt"],
        schema=corrected_schema,
    )
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V209_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "recovery_scope": "remove_unsupported_structured_output_schema_keywords_only",
        "model": v207.MODEL,
        "reasoning_effort": v207.EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "maximum_total_tokens_per_turn": v207.MAX_CANARY_TOTAL_TOKENS,
        "v208_turn_replayed": False,
        "v208_unknown_usage_preserved": True,
        "prompt_input_and_semantic_contract_unchanged": True,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "full_development_router_authorized_before_canary_pass": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "schema_recovery_audit": _record(audit_path),
        "v208_failed_attempt": predecessor["records"],
        "frozen_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
            "instructions": predecessor["predecessor"]["design"]["frozen_inputs"][
                "instructions"
            ],
        },
        "privacy": "private_source_prompts_outputs_sanitized_counts_hashes_metrics_only",
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    runtime_lock = _freeze_runtime_lock(
        root=root,
        predecessor=predecessor,
        spec_path=spec_path,
        audit_path=audit_path,
        capacity=capacity,
    )
    return {
        "root": root,
        "predecessor": predecessor,
        "paths": paths,
        "schema": corrected_schema,
        "capacity_policy": capacity["policy"],
        "capacity_audit": capacity["audit"],
        "spec": spec,
        "spec_path": spec_path,
        "schema_audit": audit_path,
        "runtime_lock": runtime_lock,
    }


def _freeze_launch_receipt(frozen: Mapping[str, Any]) -> Path:
    path = frozen["root"] / "launch-receipt.json"
    if path.exists():
        receipt = _load_json(path, "v209 launch receipt")
        if (
            receipt.get("schema_version") != V209_LAUNCH_VERSION
            or receipt.get("runtime_lock") != _record(frozen["runtime_lock"])
            or receipt.get("attempt_spec") != _record(frozen["spec_path"])
            or receipt.get("capacity_policy") != _record(frozen["capacity_policy"])
            or receipt.get("capacity_checkpoint_exists_before_launch") is not False
            or receipt.get("sidecar_exists_before_launch") is not False
            or receipt.get("output_exists_before_launch") is not False
        ):
            raise JudgeV5SelectionV209Error("v209 launch receipt drifted")
        return path
    receipt = {
        "schema_version": V209_LAUNCH_VERSION,
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
        "capacity_checkpoint_exists_before_launch": frozen["paths"]["capacity"].exists(),
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
        raise JudgeV5SelectionV209Error("v209 semantic artifacts predate launch")
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
                _load_json(Path(sidecar_record["path"]), "v209 failed sidecar")
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
        "schema_version": V209_FAILURE_VERSION,
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
        "schema_version": V209_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "full_development_router_authorized": False,
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
        + unknown * v207.MAX_CANARY_TOTAL_TOKENS,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v209(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = v207.TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v209 terminal")
    frozen = freeze_v209(output_dir=root, timeout_seconds=timeout_seconds)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = _freeze_launch_receipt(frozen)
    v207_predecessor = frozen["predecessor"]["predecessor"]
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["predecessor"]["prompt"],
                schema=frozen["schema"],
                base_instructions=v207_predecessor["instructions"],
                model=v207.MODEL,
                effort=v207.EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=v207.CANARY_CASE_COUNT,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_router_output_v209(
                    value, v207_predecessor, frozen["schema"]
                ),
            )
        accounting = _aggregate_usage([sidecar])
        gate, private_score = v208._score_router(
            output=output,
            usage=accounting["usage"],
            predecessor=v207_predecessor,
        )
        gate_path = root / "adaptive-router-canary-gate.json"
        score_path = root / "adaptive-router-canary-score.private.json"
        _write_immutable(gate_path, gate)
        _write_immutable(score_path, private_score)
        cumulative = _sum_usage(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": V209_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v209_adaptive_router_schema_recovery_canary_passed_full_development_authorized"
                if passed
                else "v209_adaptive_router_schema_recovery_quality_or_cost_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "v208_unknown_usage_preserved": True,
            "v208_turn_replayed": False,
            "launch_receipt": _record(launch_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "gate": _record(gate_path),
            "private_score": _record(score_path),
            "full_development_router_authorized": passed,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": frozen["predecessor"]["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": frozen[
                "predecessor"
            ]["terminal"]["cumulative_conservative_unknown_usage_upper_bound"],
            "required_next_artifact_path": (
                str(
                    root.parent
                    / "development-selection-v5_4-v210-adaptive-router-full-development"
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
        return _write_failure(root, frozen["predecessor"], exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["predecessor"], type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v209 router schema recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=v207.TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v209(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "full_development_router_authorized": terminal.get(
                    "full_development_router_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
                "holdout_authorized": terminal.get("holdout_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
