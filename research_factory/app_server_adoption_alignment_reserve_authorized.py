from __future__ import annotations

"""Operator-authorized 10% reserve recovery for two frozen alignment turns."""

import argparse
import asyncio
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_adoption_alignment_recovery as v2
from . import app_server_capacity_reserve as reserve
from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_adoption_alignment_reserve_authorized_recovery_v1"
LOCK_VERSION = "pif_adoption_alignment_reserve_authorized_runtime_lock_v1"
POLICY_VERSION = "pif_app_server_capacity_policy_operator_authorized_v1"
AUTHORIZATION_VERSION = "pif_app_server_capacity_reserve_operator_authorization_v1"
TERMINAL_VERSION = "pif_adoption_alignment_reserve_authorized_terminal_v1"
PROJECT_ROOT = v2.PROJECT_ROOT
PIPELINE_ROOT = v2.PIPELINE_ROOT
V2_ROOT = v2.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-adoption-output-semantic-evaluation-v1-alignment-reserve-authorized-recovery-v3"
).resolve()
TURN_NAMES = v2.TURN_NAMES
PERMUTATIONS = v2.PERMUTATIONS
MODEL = v2.MODEL
EFFORT = v2.EFFORT
MAX_TOTAL_TOKENS_PER_TURN = v2.MAX_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v2.TIMEOUT_SECONDS
USAGE_FIELDS = v2.USAGE_FIELDS
BASE_MINIMUM_RESERVE_PERCENT = 20
AUTHORIZED_MINIMUM_RESERVE_PERCENT = 10
PROJECTED_PHASE_QUOTA_POINTS = 7
AUTHORITY_SCOPE = "two_existing_neutral_ab_ba_alignment_turns_only"
AUTHORITY_STATEMENT = (
    "Kolby directly authorizes the already-frozen two-turn neutral alignment phase "
    "to use a 10 percent managed-ChatGPT minimum remaining reserve. No semantic "
    "request, model, effort, rubric, schema, threshold, extraction, support, holdout, "
    "or production change is authorized."
)


class AuthorizedAlignmentRecoveryError(RuntimeError):
    """The bounded reserve-authorized alignment recovery cannot proceed safely."""


class AuthorizedReserveCapacityError(reserve.ReserveCapacityError):
    """The operator-authorized reserve policy failed closed."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorizedAlignmentRecoveryError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise AuthorizedAlignmentRecoveryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    return v2._record(path)


def _verify_record(record: Mapping[str, Any]) -> bool:
    return v2._verify_record(record)


def _zero_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def _v2_blocker_path() -> Path:
    return (
        v2.semantic.PHASE_BOUNDARY_ROOT
        / "adoption-output-alignment-capacity-blocked-2026-07-17.json"
    )


def validate_v2_predecessor() -> dict[str, Any]:
    lock_path = V2_ROOT / "alignment" / "runtime-lock.json"
    lock = v2.verify_recovery_lock(lock_path)
    blocker_path = _v2_blocker_path()
    blocker = _load_json(blocker_path, "v2 capacity blocker")
    if (
        _record(lock_path).get("sha256")
        != "5f0ec4f63f9c0e36c20a276ae8623114bec1a028b79667c47b22f9c7106bf258"
        or _record(blocker_path).get("sha256")
        != "4c28197f88810a26b626ec7503f139737b87d397439b76ecc611bbadc84b9dfc"
        or blocker.get("state") != "blocked_on_measured_external_capacity"
        or blocker.get("recovery_semantic_attempt_started") is not False
        or blocker.get("production_mutated") is not False
        or blocker.get("capacity_decision", {}).get(
            "minimum_remaining_reserve_percent"
        )
        != BASE_MINIMUM_RESERVE_PERCENT
        or blocker.get("capacity_decision", {}).get("projected_phase_quota_points")
        != PROJECTED_PHASE_QUOTA_POINTS
        or blocker.get("recovery_runtime_lock") != _record(lock_path)
        or (V2_ROOT / "alignment" / "launch-receipt.json").exists()
        or (V2_ROOT / "terminal.json").exists()
    ):
        raise AuthorizedAlignmentRecoveryError("v2 predecessor contract drifted")
    for turn_name in TURN_NAMES:
        paths = v2.semantic._turn_paths(V2_ROOT / "alignment", turn_name)
        for forbidden in ("capacity", "sidecar", "output", "normalized"):
            if paths[forbidden].exists():
                raise AuthorizedAlignmentRecoveryError(
                    "v2 predecessor unexpectedly contains a semantic attempt"
                )
    return {
        "lock": lock,
        "lock_record": _record(lock_path),
        "blocker": blocker,
        "blocker_record": _record(blocker_path),
        "request_bindings_record": _record(
            V2_ROOT / "alignment" / "request-bindings.json"
        ),
    }


def _authorization_value(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": AUTHORIZATION_VERSION,
        "authority": "direct_user_instruction",
        "authority_scope": AUTHORITY_SCOPE,
        "authority_statement_sha256": sha256_text(AUTHORITY_STATEMENT),
        "prior_minimum_remaining_reserve_percent": BASE_MINIMUM_RESERVE_PERCENT,
        "authorized_minimum_remaining_reserve_percent": AUTHORIZED_MINIMUM_RESERVE_PERCENT,
        "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
        "last_measured_primary_remaining_percent": 19,
        "request_bytes_changed": False,
        "model_changed": False,
        "effort_changed": False,
        "prompt_changed": False,
        "rubric_changed": False,
        "schema_changed": False,
        "opaque_ids_changed": False,
        "case_order_changed": False,
        "reference_changed": False,
        "abstention_behavior_changed": False,
        "retry_count": 0,
        "frozen_quality_threshold": v2.semantic.QUALITY_THRESHOLD,
        "extraction_replay_authorized": False,
        "support_replay_authorized": False,
        "holdout_execution_authorized": False,
        "production_mutation_allowed": False,
        "v2_runtime_lock": predecessor["lock_record"],
        "v2_capacity_blocker": predecessor["blocker_record"],
    }


def _verify_authorization(path: Path, predecessor: Mapping[str, Any]) -> dict[str, Any]:
    value = _load_json(path, "operator authorization")
    expected = _authorization_value(predecessor)
    if value != expected:
        raise AuthorizedAlignmentRecoveryError("operator authorization drifted")
    return value


def _verify_embedded_record(record: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(record, Mapping) or not _verify_record(record):
        raise AuthorizedReserveCapacityError(f"authorized policy {label} drifted")
    return _load_json(Path(str(record["path"])), f"authorized policy {label}")


def load_authorized_capacity_policy(path: Path) -> dict[str, Any]:
    value = _load_json(path.expanduser().resolve(), "authorized capacity policy")
    turn_names = value.get("ordered_turn_names")
    integer_fields = (
        "minimum_remaining_reserve_percent",
        "quota_points_per_million_tokens",
        "maximum_total_tokens_per_turn",
        "phase_total_token_bound",
        "projected_phase_quota_points",
    )
    integers = {field: value.get(field) for field in integer_fields}
    if (
        value.get("schema_version") != POLICY_VERSION
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("retry_count_per_turn") != 0
        or value.get("production_mutation_allowed") is not False
        or value.get("rate_limit_reached_type_must_be_null") is not True
        or value.get("unknown_usage_hard_stop") is not True
        or value.get("operator_authorized_reserve_override") is not True
        or value.get("base_minimum_remaining_reserve_percent")
        != BASE_MINIMUM_RESERVE_PERCENT
        or not isinstance(turn_names, list)
        or tuple(turn_names) != tuple(TURN_NAMES)
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item <= 0
            for item in integers.values()
        )
        or integers["minimum_remaining_reserve_percent"]
        != AUTHORIZED_MINIMUM_RESERVE_PERCENT
        or integers["phase_total_token_bound"]
        != len(turn_names) * integers["maximum_total_tokens_per_turn"]
        or integers["projected_phase_quota_points"]
        != math.ceil(
            integers["phase_total_token_bound"]
            * integers["quota_points_per_million_tokens"]
            / 1_000_000
        )
        or integers["projected_phase_quota_points"]
        != PROJECTED_PHASE_QUOTA_POINTS
    ):
        raise AuthorizedReserveCapacityError(
            "authorized reserve capacity policy contract drifted"
        )
    output_root = Path(str(value.get("semantic_output_root") or "")).expanduser()
    if not output_root.is_absolute():
        raise AuthorizedReserveCapacityError(
            "authorized reserve capacity output root is not absolute"
        )
    audit = _verify_embedded_record(value.get("audit"), label="audit")
    authorization = _verify_embedded_record(
        value.get("operator_authorization"), label="operator authorization"
    )
    if (
        audit.get("schema_version")
        != "pif_app_server_capacity_policy_audit_v20"
        or audit.get("production_mutation_performed") is not False
        or authorization.get("schema_version") != AUTHORIZATION_VERSION
        or authorization.get("authority") != "direct_user_instruction"
        or authorization.get("authority_scope") != AUTHORITY_SCOPE
        or authorization.get("authorized_minimum_remaining_reserve_percent")
        != AUTHORIZED_MINIMUM_RESERVE_PERCENT
        or authorization.get("projected_phase_quota_points")
        != PROJECTED_PHASE_QUOTA_POINTS
        or authorization.get("production_mutation_allowed") is not False
    ):
        raise AuthorizedReserveCapacityError(
            "authorized reserve capacity evidence drifted"
        )
    return value


class AuthorizedReserveCapacityGatedClient(
    reserve.ReserveCapacityGatedCodexAppServerClient
):
    """The existing fail-closed client with an authorization-bound 10% loader."""

    def __init__(
        self,
        *,
        policy_path: Path,
        inner_factory: Callable[[], Any] = v2.semantic._inner_factory,
    ) -> None:
        self.policy_path = policy_path.expanduser().resolve()
        self.policy = load_authorized_capacity_policy(self.policy_path)
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
    bound = len(TURN_NAMES) * MAX_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(
        bound * v2.semantic.QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    if projected != PROJECTED_PHASE_QUOTA_POINTS:
        raise AuthorizedAlignmentRecoveryError("projected phase bound drifted")
    _write_stable_time(
        audit_path,
        {
            "schema_version": "pif_app_server_capacity_policy_audit_v20",
            "phase_id": "adoption_existing_output_neutral_alignment_reserve_authorized_v3",
            "created_at": now_iso(),
            "production_mutation_performed": False,
            "operator_authorized_reserve_override": True,
            "measured_basis": {
                "declared_turn_count": len(TURN_NAMES),
                "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
                "phase_total_token_bound": bound,
                "projected_phase_quota_points": projected,
                "minimum_remaining_reserve_percent": AUTHORIZED_MINIMUM_RESERVE_PERCENT,
            },
        },
        "created_at",
    )
    _write_stable_time(
        policy_path,
        {
            "schema_version": POLICY_VERSION,
            "phase_id": "adoption_existing_output_neutral_alignment_reserve_authorized_v3",
            "created_at": now_iso(),
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "retry_count_per_turn": 0,
            "production_mutation_allowed": False,
            "rate_limit_reached_type_must_be_null": True,
            "unknown_usage_hard_stop": True,
            "ordered_turn_names": list(TURN_NAMES),
            "operator_authorized_reserve_override": True,
            "base_minimum_remaining_reserve_percent": BASE_MINIMUM_RESERVE_PERCENT,
            "minimum_remaining_reserve_percent": AUTHORIZED_MINIMUM_RESERVE_PERCENT,
            "quota_points_per_million_tokens": v2.semantic.QUOTA_POINTS_PER_MILLION_TOKENS,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "semantic_output_root": str(root),
            "audit": _record(audit_path),
            "operator_authorization": _record(authorization_path),
        },
        "created_at",
    )
    load_authorized_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    return v2.semantic._turn_paths(root, turn_name)


def freeze_recovery(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    alignment_root = root / "alignment"
    lock_path = alignment_root / "runtime-lock.json"
    if lock_path.is_file():
        verify_runtime_lock(lock_path)
        return _load_frozen(root)
    if root.exists() and any(root.iterdir()):
        raise AuthorizedAlignmentRecoveryError("v3 recovery root is not empty")
    predecessor = validate_v2_predecessor()
    root.mkdir(parents=True, exist_ok=True)
    alignment_root.mkdir(parents=True, exist_ok=True)
    for turn_name in TURN_NAMES:
        _turn_paths(alignment_root, turn_name)["root"].mkdir(
            parents=True, exist_ok=True
        )
    authorization_path = root / "operator-authorization.json"
    _write_immutable(authorization_path, _authorization_value(predecessor))
    _verify_authorization(authorization_path, predecessor)
    spec_path = alignment_root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "state": "frozen_before_two_turn_reserve_authorized_alignment",
            "authority": "direct_user_instruction",
            "authority_scope": AUTHORITY_SCOPE,
            "declared_turn_count": 2,
            "turn_names": list(TURN_NAMES),
            "permutations": list(PERMUTATIONS),
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "base_minimum_remaining_reserve_percent": BASE_MINIMUM_RESERVE_PERCENT,
            "authorized_minimum_remaining_reserve_percent": AUTHORIZED_MINIMUM_RESERVE_PERCENT,
            "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
            "request_bytes_changed": False,
            "support_replayed": False,
            "extraction_replayed": False,
            "frozen_quality_threshold": v2.semantic.QUALITY_THRESHOLD,
            "holdout_execution_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    capacity = _capacity_policy(alignment_root, authorization_path)
    source_requests = _load_json(
        V2_ROOT / "alignment" / "request-bindings.json", "v2 request bindings"
    )
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": "adoption_existing_output_neutral_alignment_reserve_authorized_v3",
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 2,
        "retry_count": 0,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "base_minimum_remaining_reserve_percent": BASE_MINIMUM_RESERVE_PERCENT,
        "authorized_minimum_remaining_reserve_percent": AUTHORIZED_MINIMUM_RESERVE_PERCENT,
        "projected_phase_quota_points": PROJECTED_PHASE_QUOTA_POINTS,
        "frozen_quality_threshold": v2.semantic.QUALITY_THRESHOLD,
        "production_amortized_total_token_ratio": v2.semantic.PRODUCTION_TOKEN_RATIO,
        "runtime_adapter": _record(Path(__file__).resolve()),
        "v2_runtime_adapter": _record(Path(v2.__file__).resolve()),
        "semantic_adapter": _record(Path(v2.semantic.__file__).resolve()),
        "pinned_codex_cli": _record(v2.semantic.adoption.PINNED_CODEX),
        "predecessor_manifests": predecessor["lock"]["predecessor_manifests"],
        "v2_runtime_lock": predecessor["lock_record"],
        "v2_capacity_blocker": predecessor["blocker_record"],
        "v2_request_bindings": predecessor["request_bindings_record"],
        "frozen_source_request_records": [
            row
            for turn in source_requests["turns"]
            for row in (turn["input"], turn["prompt"], turn["schema"])
        ],
        "operator_authorization": _record(authorization_path),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "request_bytes_changed": False,
        "support_replayed": False,
        "extraction_replayed": False,
        "holdout_execution_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v3 runtime lock")
    alignment_root = path.parent.resolve()
    root = alignment_root.parent
    predecessor = validate_v2_predecessor()
    if (
        lock.get("schema_version") != LOCK_VERSION
        or lock.get("phase_id")
        != "adoption_existing_output_neutral_alignment_reserve_authorized_v3"
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count") != 0
        or lock.get("maximum_total_tokens_per_turn") != MAX_TOTAL_TOKENS_PER_TURN
        or lock.get("base_minimum_remaining_reserve_percent")
        != BASE_MINIMUM_RESERVE_PERCENT
        or lock.get("authorized_minimum_remaining_reserve_percent")
        != AUTHORIZED_MINIMUM_RESERVE_PERCENT
        or lock.get("projected_phase_quota_points") != PROJECTED_PHASE_QUOTA_POINTS
        or lock.get("frozen_quality_threshold") != v2.semantic.QUALITY_THRESHOLD
        or lock.get("production_amortized_total_token_ratio")
        != v2.semantic.PRODUCTION_TOKEN_RATIO
        or lock.get("runtime_adapter") != _record(Path(__file__).resolve())
        or lock.get("v2_runtime_adapter") != _record(Path(v2.__file__).resolve())
        or lock.get("semantic_adapter")
        != _record(Path(v2.semantic.__file__).resolve())
        or lock.get("pinned_codex_cli")
        != _record(v2.semantic.adoption.PINNED_CODEX)
        or lock.get("v2_runtime_lock") != predecessor["lock_record"]
        or lock.get("v2_capacity_blocker") != predecessor["blocker_record"]
        or lock.get("v2_request_bindings")
        != predecessor["request_bindings_record"]
        or lock.get("predecessor_manifests")
        != predecessor["lock"]["predecessor_manifests"]
        or lock.get("request_bytes_changed") is not False
        or lock.get("support_replayed") is not False
        or lock.get("extraction_replayed") is not False
        or lock.get("holdout_execution_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise AuthorizedAlignmentRecoveryError("v3 runtime lock contract drifted")
    v2.semantic._verify_predecessor_manifests(lock["predecessor_manifests"])
    for row in lock.get("frozen_source_request_records") or []:
        if not _verify_record(row):
            raise AuthorizedAlignmentRecoveryError("frozen request record drifted")
    authorization_path = root / "operator-authorization.json"
    if (
        lock.get("operator_authorization") != _record(authorization_path)
        or _verify_authorization(authorization_path, predecessor).get(
            "authorized_minimum_remaining_reserve_percent"
        )
        != AUTHORIZED_MINIMUM_RESERVE_PERCENT
    ):
        raise AuthorizedAlignmentRecoveryError("v3 authorization record drifted")
    for field in ("attempt_spec", "capacity_audit", "capacity_policy"):
        if not _verify_record(lock[field]):
            raise AuthorizedAlignmentRecoveryError(f"v3 {field} drifted")
    policy = load_authorized_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if (
        Path(str(policy["semantic_output_root"])).resolve() != alignment_root
        or tuple(policy["ordered_turn_names"]) != tuple(TURN_NAMES)
    ):
        raise AuthorizedAlignmentRecoveryError("v3 capacity path contract drifted")
    for turn_name in TURN_NAMES:
        expected = (
            alignment_root
            / "turns"
            / turn_name.replace("_", "-")
            / "capacity.json"
        )
        if _turn_paths(alignment_root, turn_name)["capacity"] != expected:
            raise AuthorizedAlignmentRecoveryError("v3 turn path contract drifted")
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    alignment_root = root / "alignment"
    source = v2._load_frozen(V2_ROOT)
    turns = []
    for turn in source["turns"]:
        turns.append(
            {
                "turn_name": turn["turn_name"],
                "permutation": turn["permutation"],
                "value": turn["value"],
                "prompt": turn["prompt"],
                "schema": turn["schema"],
                "target_paths": _turn_paths(alignment_root, turn["turn_name"]),
            }
        )
    return {
        "root": root,
        "alignment_root": alignment_root,
        "runtime_lock": alignment_root / "runtime-lock.json",
        "capacity_policy": alignment_root / "capacity-policy.json",
        "instructions": source["instructions"],
        "mapping": source["mapping"],
        "turns": turns,
    }


def _client_factory(policy_path: Path) -> AuthorizedReserveCapacityGatedClient:
    return AuthorizedReserveCapacityGatedClient(policy_path=policy_path)


async def _fresh_preflight(frozen: Mapping[str, Any]) -> dict[str, Any]:
    policy = load_authorized_capacity_policy(frozen["capacity_policy"])
    snapshot = await probe_app_server_rate_limits(
        client_factory=v2.semantic._inner_factory,
        maximum_primary_used_percent=100,
    )
    evaluation = reserve.evaluate_reserve_capacity(
        snapshot, policy=policy, remaining_turn_count=len(TURN_NAMES)
    )
    receipt = {
        **evaluation,
        "schema_version": "pif_adoption_alignment_reserve_authorized_preflight_v1",
        "checked_at": now_iso(),
        "managed_chatgpt_auth_verified": snapshot["managed_chatgpt_auth_verified"],
        "plan_type": snapshot.get("plan_type"),
        "primary_resets_at": snapshot.get("primary_resets_at"),
        "policy": _record(frozen["capacity_policy"]),
        "operator_authorization": _record(
            frozen["root"] / "operator-authorization.json"
        ),
        "thread_started": False,
        "turn_started": False,
        "sidecar_started": False,
        "production_mutated": False,
        "privacy": "capacity status policy and authorization hashes no prompts outputs credentials or thread ids",
    }
    path = frozen["alignment_root"] / "prelaunch-capacity.json"
    _write_immutable(path, receipt)
    if evaluation["cleared_for_semantic_turn"] is not True:
        raise AuthorizedReserveCapacityError(
            "authorized reserve or projected phase bound is not available"
        )
    return receipt


def _usage(path: Path) -> dict[str, int]:
    return v2.semantic._usage(
        path,
        model=MODEL,
        effort=EFFORT,
        maximum_total_tokens=MAX_TOTAL_TOKENS_PER_TURN,
    )


def _failure_terminal(
    *, root: Path, exc: BaseException, runtime_lock: Path
) -> dict[str, Any]:
    alignment_root = root / "alignment"
    attempted = unknown = 0
    usage = _zero_usage()
    sidecars = []
    for turn_name in TURN_NAMES:
        paths = _turn_paths(alignment_root, turn_name)
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        attempted += 1
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                row_usage = _usage(paths["sidecar"])
                for field in USAGE_FIELDS:
                    usage[field] += row_usage[field]
            except Exception:
                unknown += 1
        else:
            unknown += 1
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "recovery_attempted_turn_count": attempted,
        "recovery_unknown_usage_turn_count": unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "alignment_usage": usage,
        "sidecars": sidecars,
        "authorized_minimum_remaining_reserve_percent": AUTHORIZED_MINIMUM_RESERVE_PERCENT,
        "request_bytes_changed": False,
        "support_replayed": False,
        "extraction_replayed": False,
        "development_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "holdout_executed": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(runtime_lock),
    }
    _write_stable_time(alignment_root / "terminal.json", terminal, "terminal_at")
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
        return _load_json(root / "terminal.json", "v3 terminal")
    frozen = freeze_recovery(output_dir=root)
    alignment_root = frozen["alignment_root"]
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = alignment_root / "launch-receipt.json"
    if launch_path.exists():
        return _failure_terminal(
            root=root,
            exc=AuthorizedAlignmentRecoveryError("v3 launch exists; replay prohibited"),
            runtime_lock=frozen["runtime_lock"],
        )
    try:
        preflight = await _fresh_preflight(frozen)
    except BaseException as exc:
        return _failure_terminal(root=root, exc=exc, runtime_lock=frozen["runtime_lock"])
    _write_stable_time(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "declared_turn_count": 2,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "authorized_minimum_remaining_reserve_percent": AUTHORIZED_MINIMUM_RESERVE_PERCENT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "operator_authorization": _record(root / "operator-authorization.json"),
            "prelaunch_capacity": _record(
                alignment_root / "prelaunch-capacity.json"
            ),
            "prelaunch_cleared": preflight["cleared_for_semantic_turn"],
            "request_bytes_changed": False,
            "support_replayed": False,
            "extraction_replayed": False,
            "managed_chatgpt_auth_only": True,
            "holdout_execution_authorized": False,
            "production_mutation_allowed": False,
        },
        "launched_at",
    )
    started = time.monotonic()
    try:
        normalized = []
        usages = []
        async with client_factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                paths = turn["target_paths"]
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=frozen["instructions"],
                    prompt=turn["prompt"],
                    output_schema=turn["schema"],
                    cwd=PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=len(turn["value"]["cases"][0]["witnesses"]),
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(result.output, Mapping):
                    raise AuthorizedAlignmentRecoveryError(
                        f"alignment turn did not complete: {turn['turn_name']}"
                    )
                usages.append(_usage(paths["sidecar"]))
                errors = v2.semantic.judge.validate_neutral_alignment_output(
                    result.output, turn["value"]
                )
                if errors:
                    raise AuthorizedAlignmentRecoveryError(
                        "post-return neutral alignment validation failed: "
                        + "; ".join(sorted(set(errors)))
                    )
                projected = v2.semantic.judge.normalize_neutral_alignment_output(
                    result.output, turn["value"]
                )
                _write_immutable(paths["normalized"], projected)
                normalized.append(projected)
        score = v2.semantic.score_alignment(
            base=normalized[0], canary=normalized[1], mapping=frozen["mapping"]
        )
        score_path = alignment_root / "alignment-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            winner = v2.semantic._winner(v2.SOURCE_ROOT, score_path)
            winner["schema_version"] = "pif_adoption_semantic_development_winner_v3"
            winner["configuration"]["reserve_authorized_runtime_lock"] = _record(
                frozen["runtime_lock"]
            )
            winner["configuration"]["operator_authorization"] = _record(
                root / "operator-authorization.json"
            )
            winner["holdout_execution_started"] = False
            _write_stable_time(winner_path, winner, "frozen_at")
            winner_record = _record(winner_path)
        alignment_usage = {
            field: sum(row[field] for row in usages) for field in USAGE_FIELDS
        }
        support_terminal = _load_json(
            v2.SOURCE_ROOT / "support" / "terminal.json", "source support terminal"
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
            "recovery_attempted_turn_count": 2,
            "measured_alignment_turn_count": 2,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "support_usage": support_usage,
            "alignment_usage": alignment_usage,
            "total_judge_usage": total_judge_usage,
            "authorized_minimum_remaining_reserve_percent": AUTHORIZED_MINIMUM_RESERVE_PERCENT,
            "request_bytes_changed": False,
            "support_replayed": False,
            "extraction_replayed": False,
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
            "holdout_executed": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "production_amortized_total_token_ratio": v2.semantic.PRODUCTION_TOKEN_RATIO,
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "development_winner": winner_record,
            "source_support_terminal": _record(
                v2.SOURCE_ROOT / "support" / "terminal.json"
            ),
            "sidecars": [
                _record(turn["target_paths"]["sidecar"])
                for turn in frozen["turns"]
            ],
            "runtime_lock": _record(frozen["runtime_lock"]),
            "operator_authorization": _record(root / "operator-authorization.json"),
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
            return _load_json(alignment_root / "terminal.json", "v3 terminal")
        return _failure_terminal(root=root, exc=exc, runtime_lock=frozen["runtime_lock"])


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the operator-authorized 10 percent reserve alignment recovery"
    )
    parser.add_argument("action", choices=("freeze", "verify", "run"))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    root = Path(args.output_dir)
    if args.action == "freeze":
        frozen = freeze_recovery(output_dir=root)
        result = {
            "state": "frozen_before_two_turn_reserve_authorized_alignment",
            "root": str(frozen["root"]),
            "turn_count": len(frozen["turns"]),
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
