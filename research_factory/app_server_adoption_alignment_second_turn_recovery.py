from __future__ import annotations

"""Adopt one structural ID projection and run the never-started alignment turn."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_adoption_alignment_reserve_authorized as v3
from . import app_server_capacity_reserve as reserve
from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_adoption_alignment_second_turn_recovery_v1"
LOCK_VERSION = "pif_adoption_alignment_second_turn_runtime_lock_v1"
POLICY_VERSION = "pif_app_server_capacity_policy_operator_authorized_v1"
TERMINAL_VERSION = "pif_adoption_alignment_second_turn_terminal_v1"
PROJECT_ROOT = v3.PROJECT_ROOT
PIPELINE_ROOT = v3.PIPELINE_ROOT
V3_ROOT = v3.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-adoption-output-semantic-evaluation-v1-alignment-reserve-authorized-second-turn-recovery-v4"
).resolve()
FIRST_TURN_NAME = v3.TURN_NAMES[0]
SECOND_TURN_NAME = v3.TURN_NAMES[1]
MODEL = v3.MODEL
EFFORT = v3.EFFORT
MAX_TOTAL_TOKENS = v3.MAX_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v3.TIMEOUT_SECONDS
USAGE_FIELDS = v3.USAGE_FIELDS
MINIMUM_RESERVE_PERCENT = v3.AUTHORIZED_MINIMUM_RESERVE_PERCENT
PROJECTED_QUOTA_POINTS = math.ceil(
    MAX_TOTAL_TOKENS * v3.v2.semantic.QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
)
AUTHORITY_SCOPE = "never_started_second_neutral_alignment_turn_only"
ALLOWED_RAW_ERRORS = ["case_0_alignment_partition_mismatch"]


class SecondTurnRecoveryError(RuntimeError):
    """The one-turn alignment continuation cannot proceed safely."""


class SecondTurnCapacityError(reserve.ReserveCapacityError):
    """The one-turn operator-authorized capacity policy failed closed."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SecondTurnRecoveryError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise SecondTurnRecoveryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    return v3._record(path)


def _verify_record(record: Mapping[str, Any]) -> bool:
    return v3._verify_record(record)


def _zero_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    return v3._turn_paths(root, turn_name)


def validate_v3_predecessor() -> dict[str, Any]:
    root = V3_ROOT
    alignment = root / "alignment"
    lock_path = alignment / "runtime-lock.json"
    terminal_path = root / "terminal.json"
    first_paths = _turn_paths(alignment, FIRST_TURN_NAME)
    second_paths = _turn_paths(alignment, SECOND_TURN_NAME)
    lock = v3.verify_runtime_lock(lock_path)
    terminal = _load_json(terminal_path, "v3 terminal")
    first_sidecar = _load_json(first_paths["sidecar"], "v3 first sidecar")
    first_usage = first_sidecar.get("usage") or {}
    if (
        terminal.get("state") != "inactive_incomplete_recovery_required"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("error_class") != "AuthorizedAlignmentRecoveryError"
        or terminal.get("recovery_attempted_turn_count") != 1
        or terminal.get("recovery_unknown_usage_turn_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("runtime_lock") != _record(lock_path)
        or first_sidecar.get("state") != "completed"
        or first_sidecar.get("status") != "completed"
        or first_sidecar.get("auth_type") != "chatgpt"
        or first_sidecar.get("model") != MODEL
        or first_sidecar.get("effort") != EFFORT
        or first_sidecar.get("usage_status") != "measured"
        or first_sidecar.get("usage_complete") is not True
        or first_sidecar.get("error_class") is not None
        or first_usage.get("total_tokens") != 77_168
        or not first_paths["capacity"].is_file()
        or not first_paths["output"].is_file()
        or first_paths["normalized"].exists()
    ):
        raise SecondTurnRecoveryError("v3 measured predecessor contract drifted")
    if any(second_paths[field].exists() for field in ("capacity", "sidecar", "output", "normalized")):
        raise SecondTurnRecoveryError("v3 second turn was not actually unstarted")
    source = v3._load_frozen(root)
    first_value = source["turns"][0]["value"]
    first_output = _load_json(first_paths["output"], "v3 first output")
    errors = v3.v2.semantic.judge.validate_neutral_alignment_output(
        first_output, first_value
    )
    if errors != ALLOWED_RAW_ERRORS:
        raise SecondTurnRecoveryError("v3 failure is not the bounded ID partition defect")
    return {
        "lock": lock,
        "lock_record": _record(lock_path),
        "terminal": terminal,
        "terminal_record": _record(terminal_path),
        "first_sidecar": first_sidecar,
        "first_sidecar_record": _record(first_paths["sidecar"]),
        "first_output": first_output,
        "first_output_record": _record(first_paths["output"]),
        "first_capacity_record": _record(first_paths["capacity"]),
        "v3_authorization_record": _record(root / "operator-authorization.json"),
        "source": source,
    }


def _semantic_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(value)
    for case in payload.get("cases") or []:
        case.pop("unpaired_witness_ids", None)
    return payload


def project_nonsemantic_id_coverage(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_errors = v3.v2.semantic.judge.validate_neutral_alignment_output(
        output, alignment_input
    )
    if raw_errors not in ([], ALLOWED_RAW_ERRORS):
        raise SecondTurnRecoveryError("alignment output has a non-projectable defect")
    projected = copy.deepcopy(output)
    case_audits = []
    if raw_errors:
        expected_by_case = {
            str(case["case_id"]): {
                str(witness["witness_id"]) for witness in case["witnesses"]
            }
            for case in alignment_input["cases"]
        }
        for case in projected["cases"]:
            expected = expected_by_case[str(case["case_id"])]
            used = {
                witness_id
                for pair in case["alignment_pairs"]
                for witness_id in (pair["witness_id_1"], pair["witness_id_2"])
            }
            before = list(case["unpaired_witness_ids"])
            after = sorted(expected - used)
            case["unpaired_witness_ids"] = after
            case_audits.append(
                {
                    "case_id_sha256": sha256_text(str(case["case_id"])),
                    "expected_witness_count": len(expected),
                    "paired_witness_count": len(used),
                    "unpaired_before_count": len(before),
                    "unpaired_after_count": len(after),
                    "paired_unpaired_overlap_before_count": len(used & set(before)),
                    "missing_before_count": len(expected - (used | set(before))),
                    "semantic_payload_changed": False,
                }
            )
    projected_errors = v3.v2.semantic.judge.validate_neutral_alignment_output(
        projected, alignment_input
    )
    if projected_errors:
        raise SecondTurnRecoveryError("ID coverage projection did not validate")
    if _semantic_payload(projected) != _semantic_payload(output):
        raise SecondTurnRecoveryError("ID coverage projection changed semantic output")
    audit = {
        "schema_version": SCHEMA_VERSION,
        "raw_error_classes": raw_errors,
        "projection_applied": bool(raw_errors),
        "projected_field": "unpaired_witness_ids" if raw_errors else None,
        "projection_rule": (
            "all_input_witness_ids_minus_alignment_pair_witness_ids"
            if raw_errors
            else None
        ),
        "semantic_payload_changed": False,
        "case_audits": case_audits,
        "privacy": "counts and hashes only no source or event text",
    }
    return projected, audit


def _authorization_value(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "authority": "direct_user_instruction_continuation",
        "authority_scope": AUTHORITY_SCOPE,
        "authority_source": predecessor["v3_authorization_record"],
        "v3_terminal": predecessor["terminal_record"],
        "completed_first_turn_replayed": False,
        "never_started_second_turn_declared_attempt_count": 1,
        "minimum_remaining_reserve_percent": MINIMUM_RESERVE_PERCENT,
        "projected_phase_quota_points": PROJECTED_QUOTA_POINTS,
        "request_bytes_changed": False,
        "model_changed": False,
        "effort_changed": False,
        "prompt_changed": False,
        "rubric_changed": False,
        "schema_changed": False,
        "quality_threshold_changed": False,
        "frozen_quality_threshold": v3.v2.semantic.QUALITY_THRESHOLD,
        "structural_projection_field": "unpaired_witness_ids",
        "semantic_projection_allowed": False,
        "support_replayed": False,
        "extraction_replayed": False,
        "holdout_execution_authorized": False,
        "production_mutation_allowed": False,
    }


def _verify_embedded_record(record: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(record, Mapping) or not _verify_record(record):
        raise SecondTurnCapacityError(f"second-turn policy {label} drifted")
    return _load_json(Path(str(record["path"])), f"second-turn policy {label}")


def load_capacity_policy(path: Path) -> dict[str, Any]:
    value = _load_json(path.expanduser().resolve(), "second-turn capacity policy")
    if (
        value.get("schema_version") != POLICY_VERSION
        or value.get("phase_id")
        != "adoption_existing_output_alignment_second_turn_recovery_v4"
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("retry_count_per_turn") != 0
        or value.get("production_mutation_allowed") is not False
        or value.get("rate_limit_reached_type_must_be_null") is not True
        or value.get("unknown_usage_hard_stop") is not True
        or value.get("operator_authorized_reserve_override") is not True
        or value.get("base_minimum_remaining_reserve_percent") != 20
        or value.get("minimum_remaining_reserve_percent") != MINIMUM_RESERVE_PERCENT
        or value.get("ordered_turn_names") != [SECOND_TURN_NAME]
        or value.get("quota_points_per_million_tokens")
        != v3.v2.semantic.QUOTA_POINTS_PER_MILLION_TOKENS
        or value.get("maximum_total_tokens_per_turn") != MAX_TOTAL_TOKENS
        or value.get("phase_total_token_bound") != MAX_TOTAL_TOKENS
        or value.get("projected_phase_quota_points") != PROJECTED_QUOTA_POINTS
    ):
        raise SecondTurnCapacityError("second-turn capacity policy contract drifted")
    output_root = Path(str(value.get("semantic_output_root") or "")).expanduser()
    if not output_root.is_absolute():
        raise SecondTurnCapacityError("second-turn capacity root is not absolute")
    audit = _verify_embedded_record(value.get("audit"), label="audit")
    authorization = _verify_embedded_record(
        value.get("operator_authorization"), label="authorization"
    )
    if (
        audit.get("schema_version")
        != "pif_app_server_capacity_policy_audit_v20"
        or audit.get("production_mutation_performed") is not False
        or authorization.get("authority_scope") != AUTHORITY_SCOPE
        or authorization.get("minimum_remaining_reserve_percent")
        != MINIMUM_RESERVE_PERCENT
        or authorization.get("production_mutation_allowed") is not False
    ):
        raise SecondTurnCapacityError("second-turn capacity evidence drifted")
    return value


class SecondTurnCapacityClient(reserve.ReserveCapacityGatedCodexAppServerClient):
    def __init__(
        self,
        *,
        policy_path: Path,
        inner_factory: Callable[[], Any] = v3.v2.semantic._inner_factory,
    ) -> None:
        self.policy_path = policy_path.expanduser().resolve()
        self.policy = load_capacity_policy(self.policy_path)
        self.policy_sha256 = reserve._sha256_file(self.policy_path)
        self.inner_factory = inner_factory
        self.inner_context: Optional[Any] = None
        self.inner: Optional[Any] = None
        self._semantic_turn_lock = asyncio.Lock()
        self._sticky_failure: Optional[str] = None
        self.capacity_checks: list[dict[str, Any]] = []


def _capacity_policy(root: Path, authorization_path: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    _write_stable_time(
        audit_path,
        {
            "schema_version": "pif_app_server_capacity_policy_audit_v20",
            "phase_id": "adoption_existing_output_alignment_second_turn_recovery_v4",
            "created_at": now_iso(),
            "production_mutation_performed": False,
            "operator_authorized_reserve_override": True,
            "measured_basis": {
                "declared_turn_count": 1,
                "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
                "phase_total_token_bound": MAX_TOTAL_TOKENS,
                "projected_phase_quota_points": PROJECTED_QUOTA_POINTS,
                "minimum_remaining_reserve_percent": MINIMUM_RESERVE_PERCENT,
            },
        },
        "created_at",
    )
    _write_stable_time(
        policy_path,
        {
            "schema_version": POLICY_VERSION,
            "phase_id": "adoption_existing_output_alignment_second_turn_recovery_v4",
            "created_at": now_iso(),
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "retry_count_per_turn": 0,
            "production_mutation_allowed": False,
            "rate_limit_reached_type_must_be_null": True,
            "unknown_usage_hard_stop": True,
            "ordered_turn_names": [SECOND_TURN_NAME],
            "operator_authorized_reserve_override": True,
            "base_minimum_remaining_reserve_percent": 20,
            "minimum_remaining_reserve_percent": MINIMUM_RESERVE_PERCENT,
            "quota_points_per_million_tokens": v3.v2.semantic.QUOTA_POINTS_PER_MILLION_TOKENS,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
            "projected_phase_quota_points": PROJECTED_QUOTA_POINTS,
            "semantic_output_root": str(root),
            "audit": _record(audit_path),
            "operator_authorization": _record(authorization_path),
        },
        "created_at",
    )
    load_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def freeze_recovery(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    alignment_root = root / "alignment"
    lock_path = alignment_root / "runtime-lock.json"
    if lock_path.is_file():
        verify_runtime_lock(lock_path)
        return _load_frozen(root)
    if root.exists() and any(root.iterdir()):
        raise SecondTurnRecoveryError("v4 recovery root is not empty")
    predecessor = validate_v3_predecessor()
    root.mkdir(parents=True, exist_ok=True)
    alignment_root.mkdir(parents=True, exist_ok=True)
    target_paths = _turn_paths(alignment_root, SECOND_TURN_NAME)
    target_paths["root"].mkdir(parents=True, exist_ok=True)
    authorization_path = root / "operator-authorization.json"
    _write_immutable(authorization_path, _authorization_value(predecessor))
    first_projected, first_audit = project_nonsemantic_id_coverage(
        predecessor["first_output"], predecessor["source"]["turns"][0]["value"]
    )
    first_projected_path = alignment_root / "first-permutation-projected.private.json"
    first_audit_path = alignment_root / "first-permutation-projection-audit.json"
    _write_immutable(first_projected_path, first_projected)
    _write_immutable(first_audit_path, first_audit)
    first_normalized = v3.v2.semantic.judge.normalize_neutral_alignment_output(
        first_projected, predecessor["source"]["turns"][0]["value"]
    )
    first_normalized_path = alignment_root / "first-permutation-normalized.private.json"
    _write_immutable(first_normalized_path, first_normalized)
    spec_path = alignment_root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "state": "frozen_before_never_started_second_alignment_turn",
            "authority_scope": AUTHORITY_SCOPE,
            "declared_turn_count": 1,
            "turn_name": SECOND_TURN_NAME,
            "permutation": v3.PERMUTATIONS[1],
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "minimum_remaining_reserve_percent": MINIMUM_RESERVE_PERCENT,
            "projected_phase_quota_points": PROJECTED_QUOTA_POINTS,
            "completed_first_turn_replayed": False,
            "request_bytes_changed": False,
            "semantic_projection_performed": False,
            "structural_projection_field": "unpaired_witness_ids",
            "support_replayed": False,
            "extraction_replayed": False,
            "frozen_quality_threshold": v3.v2.semantic.QUALITY_THRESHOLD,
            "holdout_execution_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    capacity = _capacity_policy(alignment_root, authorization_path)
    second_source_paths = v3.v2.semantic._turn_paths(
        v3.v2.SOURCE_ROOT / "alignment", SECOND_TURN_NAME
    )
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": "adoption_existing_output_alignment_second_turn_recovery_v4",
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "maximum_total_tokens": MAX_TOTAL_TOKENS,
        "minimum_remaining_reserve_percent": MINIMUM_RESERVE_PERCENT,
        "projected_phase_quota_points": PROJECTED_QUOTA_POINTS,
        "frozen_quality_threshold": v3.v2.semantic.QUALITY_THRESHOLD,
        "production_amortized_total_token_ratio": v3.v2.semantic.PRODUCTION_TOKEN_RATIO,
        "runtime_adapter": _record(Path(__file__).resolve()),
        "v3_runtime_adapter": _record(Path(v3.__file__).resolve()),
        "pinned_codex_cli": _record(v3.v2.semantic.adoption.PINNED_CODEX),
        "v3_runtime_lock": predecessor["lock_record"],
        "v3_terminal": predecessor["terminal_record"],
        "v3_first_capacity": predecessor["first_capacity_record"],
        "v3_first_sidecar": predecessor["first_sidecar_record"],
        "v3_first_output": predecessor["first_output_record"],
        "v3_authorization": predecessor["v3_authorization_record"],
        "second_request_records": [
            _record(second_source_paths[field]) for field in ("input", "prompt", "schema")
        ],
        "operator_authorization": _record(authorization_path),
        "first_projection": _record(first_projected_path),
        "first_projection_audit": _record(first_audit_path),
        "first_normalized": _record(first_normalized_path),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "completed_first_turn_replayed": False,
        "request_bytes_changed": False,
        "semantic_projection_performed": False,
        "support_replayed": False,
        "extraction_replayed": False,
        "holdout_execution_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v4 runtime lock")
    alignment_root = path.parent.resolve()
    root = alignment_root.parent
    predecessor = validate_v3_predecessor()
    if (
        lock.get("schema_version") != LOCK_VERSION
        or lock.get("phase_id")
        != "adoption_existing_output_alignment_second_turn_recovery_v4"
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("maximum_total_tokens") != MAX_TOTAL_TOKENS
        or lock.get("minimum_remaining_reserve_percent") != MINIMUM_RESERVE_PERCENT
        or lock.get("projected_phase_quota_points") != PROJECTED_QUOTA_POINTS
        or lock.get("frozen_quality_threshold") != v3.v2.semantic.QUALITY_THRESHOLD
        or lock.get("runtime_adapter") != _record(Path(__file__).resolve())
        or lock.get("v3_runtime_adapter") != _record(Path(v3.__file__).resolve())
        or lock.get("pinned_codex_cli")
        != _record(v3.v2.semantic.adoption.PINNED_CODEX)
        or lock.get("v3_runtime_lock") != predecessor["lock_record"]
        or lock.get("v3_terminal") != predecessor["terminal_record"]
        or lock.get("v3_first_capacity") != predecessor["first_capacity_record"]
        or lock.get("v3_first_sidecar") != predecessor["first_sidecar_record"]
        or lock.get("v3_first_output") != predecessor["first_output_record"]
        or lock.get("v3_authorization") != predecessor["v3_authorization_record"]
        or lock.get("completed_first_turn_replayed") is not False
        or lock.get("request_bytes_changed") is not False
        or lock.get("semantic_projection_performed") is not False
        or lock.get("support_replayed") is not False
        or lock.get("extraction_replayed") is not False
        or lock.get("holdout_execution_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise SecondTurnRecoveryError("v4 runtime lock contract drifted")
    for field in (
        "operator_authorization",
        "first_projection",
        "first_projection_audit",
        "first_normalized",
        "attempt_spec",
        "capacity_audit",
        "capacity_policy",
    ):
        if not _verify_record(lock[field]):
            raise SecondTurnRecoveryError(f"v4 {field} drifted")
    if any(not _verify_record(row) for row in lock["second_request_records"]):
        raise SecondTurnRecoveryError("v4 second request drifted")
    if _load_json(root / "operator-authorization.json", "v4 authorization") != _authorization_value(
        predecessor
    ):
        raise SecondTurnRecoveryError("v4 authorization drifted")
    projected = _load_json(
        alignment_root / "first-permutation-projected.private.json",
        "v4 first projection",
    )
    expected_projected, expected_audit = project_nonsemantic_id_coverage(
        predecessor["first_output"], predecessor["source"]["turns"][0]["value"]
    )
    if projected != expected_projected or _load_json(
        alignment_root / "first-permutation-projection-audit.json",
        "v4 first projection audit",
    ) != expected_audit:
        raise SecondTurnRecoveryError("v4 deterministic projection drifted")
    policy = load_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if Path(str(policy["semantic_output_root"])).resolve() != alignment_root:
        raise SecondTurnRecoveryError("v4 capacity root drifted")
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    alignment_root = root / "alignment"
    source = v3._load_frozen(V3_ROOT)
    second = source["turns"][1]
    return {
        "root": root,
        "alignment_root": alignment_root,
        "runtime_lock": alignment_root / "runtime-lock.json",
        "capacity_policy": alignment_root / "capacity-policy.json",
        "instructions": source["instructions"],
        "mapping": source["mapping"],
        "first_value": source["turns"][0]["value"],
        "first_normalized": _load_json(
            alignment_root / "first-permutation-normalized.private.json",
            "first normalized output",
        ),
        "second": {
            "turn_name": SECOND_TURN_NAME,
            "value": second["value"],
            "prompt": second["prompt"],
            "schema": second["schema"],
            "target_paths": _turn_paths(alignment_root, SECOND_TURN_NAME),
        },
    }


def _client_factory(policy_path: Path) -> SecondTurnCapacityClient:
    return SecondTurnCapacityClient(policy_path=policy_path)


async def _fresh_preflight(frozen: Mapping[str, Any]) -> dict[str, Any]:
    policy = load_capacity_policy(frozen["capacity_policy"])
    snapshot = await probe_app_server_rate_limits(
        client_factory=v3.v2.semantic._inner_factory,
        maximum_primary_used_percent=100,
    )
    evaluation = reserve.evaluate_reserve_capacity(
        snapshot, policy=policy, remaining_turn_count=1
    )
    receipt = {
        **evaluation,
        "schema_version": "pif_adoption_alignment_second_turn_preflight_v1",
        "checked_at": now_iso(),
        "managed_chatgpt_auth_verified": snapshot["managed_chatgpt_auth_verified"],
        "plan_type": snapshot.get("plan_type"),
        "primary_resets_at": snapshot.get("primary_resets_at"),
        "policy": _record(frozen["capacity_policy"]),
        "thread_started": False,
        "turn_started": False,
        "sidecar_started": False,
        "production_mutated": False,
    }
    _write_immutable(frozen["alignment_root"] / "prelaunch-capacity.json", receipt)
    if evaluation["cleared_for_semantic_turn"] is not True:
        raise SecondTurnCapacityError("second-turn capacity bound is not available")
    return receipt


def _usage(path: Path) -> dict[str, int]:
    return v3.v2.semantic._usage(
        path,
        model=MODEL,
        effort=EFFORT,
        maximum_total_tokens=MAX_TOTAL_TOKENS,
    )


def _failure_terminal(root: Path, exc: BaseException, lock_path: Path) -> dict[str, Any]:
    paths = _turn_paths(root / "alignment", SECOND_TURN_NAME)
    attempted = paths["capacity"].exists() or paths["sidecar"].exists()
    unknown = False
    usage = _zero_usage()
    sidecars = []
    if paths["sidecar"].is_file():
        sidecars.append(_record(paths["sidecar"]))
        try:
            usage = _usage(paths["sidecar"])
        except Exception:
            unknown = True
    elif attempted:
        unknown = True
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "second_turn_attempted_count": int(attempted),
        "unknown_usage_turn_count": int(unknown),
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": not unknown,
        "second_turn_usage": usage,
        "sidecars": sidecars,
        "completed_first_turn_replayed": False,
        "request_bytes_changed": False,
        "semantic_projection_performed": False,
        "support_replayed": False,
        "extraction_replayed": False,
        "development_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "holdout_executed": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(lock_path),
    }
    _write_stable_time(root / "alignment" / "terminal.json", terminal, "terminal_at")
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_recovery(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "v4 terminal")
    frozen = freeze_recovery(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    alignment_root = frozen["alignment_root"]
    launch_path = alignment_root / "launch-receipt.json"
    if launch_path.exists():
        return _failure_terminal(
            root,
            SecondTurnRecoveryError("v4 launch exists; replay prohibited"),
            frozen["runtime_lock"],
        )
    try:
        preflight = await _fresh_preflight(frozen)
    except BaseException as exc:
        return _failure_terminal(root, exc, frozen["runtime_lock"])
    _write_stable_time(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "declared_turn_count": 1,
            "turn_name": SECOND_TURN_NAME,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "prelaunch_capacity": _record(alignment_root / "prelaunch-capacity.json"),
            "prelaunch_cleared": preflight["cleared_for_semantic_turn"],
            "completed_first_turn_replayed": False,
            "request_bytes_changed": False,
            "support_replayed": False,
            "extraction_replayed": False,
            "holdout_execution_authorized": False,
            "production_mutation_allowed": False,
        },
        "launched_at",
    )
    started = time.monotonic()
    try:
        second = frozen["second"]
        paths = second["target_paths"]
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["instructions"],
                prompt=second["prompt"],
                output_schema=second["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=len(second["value"]["cases"][0]["witnesses"]),
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise SecondTurnRecoveryError("second alignment turn did not complete")
        second_usage = _usage(paths["sidecar"])
        second_projected, second_audit = project_nonsemantic_id_coverage(
            result.output, second["value"]
        )
        second_projected_path = alignment_root / "second-permutation-projected.private.json"
        second_audit_path = alignment_root / "second-permutation-projection-audit.json"
        _write_immutable(second_projected_path, second_projected)
        _write_immutable(second_audit_path, second_audit)
        second_normalized = v3.v2.semantic.judge.normalize_neutral_alignment_output(
            second_projected, second["value"]
        )
        _write_immutable(paths["normalized"], second_normalized)
        score = v3.v2.semantic.score_alignment(
            base=frozen["first_normalized"],
            canary=second_normalized,
            mapping=frozen["mapping"],
        )
        score_path = alignment_root / "alignment-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            winner = v3.v2.semantic._winner(v3.v2.SOURCE_ROOT, score_path)
            winner["schema_version"] = "pif_adoption_semantic_development_winner_v4"
            winner["configuration"]["second_turn_recovery_runtime_lock"] = _record(
                frozen["runtime_lock"]
            )
            winner["holdout_execution_started"] = False
            _write_stable_time(winner_path, winner, "frozen_at")
            winner_record = _record(winner_path)
        predecessor = validate_v3_predecessor()
        first_usage = predecessor["first_sidecar"]["usage"]
        alignment_usage = {
            field: int(first_usage[field]) + int(second_usage[field])
            for field in USAGE_FIELDS
        }
        support_terminal = _load_json(
            v3.v2.SOURCE_ROOT / "support" / "terminal.json", "support terminal"
        )
        support_usage = support_terminal["usage"]
        total_judge_usage = {
            field: int(support_usage[field]) + alignment_usage[field]
            for field in USAGE_FIELDS
        }
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "completed" if passed else "inactive_incomplete_recovery_required",
            "terminal_reason": (
                "adoption_semantic_quality_passed_development_winner_frozen"
                if passed
                else "adoption_semantic_quality_gate_not_passed"
            ),
            "measured_alignment_turn_count": 2,
            "v3_first_turn_replayed": False,
            "v4_second_turn_attempted_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "first_turn_usage": first_usage,
            "second_turn_usage": second_usage,
            "alignment_usage": alignment_usage,
            "support_usage": support_usage,
            "total_judge_usage": total_judge_usage,
            "first_structural_projection": _record(
                alignment_root / "first-permutation-projection-audit.json"
            ),
            "second_structural_projection": _record(second_audit_path),
            "request_bytes_changed": False,
            "semantic_projection_performed": False,
            "support_replayed": False,
            "extraction_replayed": False,
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
            "holdout_executed": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "production_amortized_total_token_ratio": v3.v2.semantic.PRODUCTION_TOKEN_RATIO,
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "development_winner": winner_record,
            "sidecars": [
                predecessor["first_sidecar_record"],
                _record(paths["sidecar"]),
            ],
            "runtime_lock": _record(frozen["runtime_lock"]),
            "exact_next_action": (
                "request separate authorization for the already-defined untouched holdout gates"
                if passed
                else "freeze the measured semantic-quality blocker; do not replay extraction or support"
            ),
        }
        _write_stable_time(alignment_root / "terminal.json", terminal, "terminal_at")
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (alignment_root / "terminal.json").is_file():
            return _load_json(alignment_root / "terminal.json", "v4 terminal")
        return _failure_terminal(root, exc, frozen["runtime_lock"])


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the never-started second neutral alignment turn"
    )
    parser.add_argument("action", choices=("freeze", "verify", "run"))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    root = Path(args.output_dir)
    if args.action == "freeze":
        frozen = freeze_recovery(output_dir=root)
        result = {
            "state": "frozen_before_never_started_second_alignment_turn",
            "root": str(frozen["root"]),
            "turn_count": 1,
        }
    elif args.action == "verify":
        verify_runtime_lock(root / "alignment" / "runtime-lock.json")
        result = {"state": "verified", "root": str(root)}
    else:
        result = asyncio.run(
            run_recovery(output_dir=root, timeout_seconds=args.timeout_seconds)
        )
    sanitized = {
        key: result.get(key)
        for key in (
            "state",
            "terminal_reason",
            "usage_status",
            "development_quality_passed",
            "development_winner_frozen",
            "holdout_authorized",
            "holdout_executed",
            "production_mutated",
            "root",
            "turn_count",
        )
        if key in result
    }
    print(json.dumps(sanitized, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
