from __future__ import annotations

"""Preserve v22 pre-semantic failure and authorize corrected v23 ordering."""

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_diagnostic as diagnostic
from .app_server_capacity_policy_v21 import (
    CapacityPolicyV21Error,
    audit_v20_checkpoint_validator_failure,
)
from .app_server_capacity_policy_v22 import CALIBRATION_TURN_NAMES
from .app_server_capacity_reserve import (
    RESERVE_CAPACITY_POLICY_VERSION,
    evaluate_reserve_capacity,
    load_reserve_capacity_policy,
)
from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits
from .app_server_judge_v5_fresh_calibration_v22 import (
    REFERENCE_ROOT,
    load_frozen_fixture_reference_v21,
)
from .app_server_judge_v5_fresh_calibration_v23 import DEFAULT_OUTPUT_ROOT
from .app_server_judge_v5_reference_adjudication_v21 import (
    build_reserve_capacity_checkpoint_validator,
)
from .app_server_runtime_lock_v22 import verify_runtime_lock_v22
from .util import now_iso


PRESEMANTIC_FAILURE_AUDIT_VERSION = (
    "pif_app_server_calibration_presemantic_failure_audit_v23"
)
LAUNCH_RECEIPT_VERSION = "pif_app_server_calibration_launch_receipt_v23"
DEFAULT_CONTROL_ROOT = Path(
    "work/app-server-development-v2/unattended-control-v23"
).resolve()
DEFAULT_FAILURE_AUDIT = DEFAULT_CONTROL_ROOT / "presemantic-failure-audit-v22.json"
DEFAULT_POLICY = DEFAULT_CONTROL_ROOT / "capacity-policy-v23.json"
SOURCE_POLICY = Path(
    "work/app-server-development-v2/unattended-control-v22/capacity-policy-v22.json"
).resolve()
V22_CONTROL_ROOT = Path(
    "work/app-server-development-v2/unattended-control-v22"
).resolve()
V22_RUNTIME_LOCK = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v22.json"
).resolve()
V22_OUTPUT_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "judge-calibration-v5_4-reference-v2-capacity-v22"
).resolve()


class CapacityPolicyV23Error(RuntimeError):
    """The v22 failure or v23 capacity authorization is unsafe."""


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(path: Path) -> dict[str, Any]:
    target = path.expanduser().resolve()
    if not target.is_file():
        raise CapacityPolicyV23Error("required v23 lineage artifact is missing")
    return {
        "path": str(target),
        "sha256": _sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityPolicyV23Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise CapacityPolicyV23Error(f"{purpose} is not an object")
    return value


def _write_immutable(path: Path, value: Any) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = target.open("xb")
    except FileExistsError as exc:
        raise CapacityPolicyV23Error("immutable v23 artifact already exists") from exc
    with handle:
        handle.write(_canonical_bytes(value))
        handle.flush()
        os.fsync(handle.fileno())


def build_v22_presemantic_failure_audit() -> dict[str, Any]:
    lock_report = verify_runtime_lock_v22(
        repo_root=Path.cwd(), manifest_path=V22_RUNTIME_LOCK
    )
    launch_path = V22_CONTROL_ROOT / "launch-receipt-v22.json"
    launch = _load(launch_path, "v22 launch receipt")
    if (
        launch.get("schema_version")
        != "pif_app_server_calibration_launch_receipt_v22"
        or launch.get("status")
        != "authorized_for_one_fresh_v22_calibration_phase"
        or launch.get("output_roots_absent") is not True
        or launch.get("retry_count_per_turn") != 0
        or launch.get("selection_authorized_before_calibration") is not False
        or launch.get("holdout_authorized") is not False
        or launch.get("production_mutation_performed") is not False
        or (launch.get("live_capacity") or {}).get(
            "managed_chatgpt_auth_verified"
        )
        is not True
        or (launch.get("live_capacity") or {}).get("thread_started") is not False
        or (launch.get("live_capacity") or {}).get("turn_started") is not False
        or (launch.get("reserve_evaluation") or {}).get(
            "cleared_for_semantic_turn"
        )
        is not True
    ):
        raise CapacityPolicyV23Error("v22 launch receipt drifted")
    if V22_OUTPUT_ROOT.exists():
        raise CapacityPolicyV23Error("v22 semantic root exists; failure is not presemantic")

    original_validator = diagnostic._validate_capacity_checkpoint
    diagnostic._validate_capacity_checkpoint = (
        build_reserve_capacity_checkpoint_validator(SOURCE_POLICY)
    )
    reproduced = None
    try:
        try:
            audit_v20_checkpoint_validator_failure()
        except CapacityPolicyV21Error as exc:
            reproduced = str(exc)
    finally:
        diagnostic._validate_capacity_checkpoint = original_validator
    if reproduced != "v20 local validator failure did not reproduce":
        raise CapacityPolicyV23Error("v22 pre-validation ordering failure did not reproduce")
    reference = load_frozen_fixture_reference_v21(REFERENCE_ROOT)
    return {
        "schema_version": PRESEMANTIC_FAILURE_AUDIT_VERSION,
        "created_at": now_iso(),
        "state": "failed_before_semantic_attempt",
        "terminal_reason": "local_reference_prevalidation_ordering_failed",
        "classification": "infrastructure_or_local_protocol_attempt_failed",
        "error_class": "CapacityPolicyV21Error",
        "root_cause_code": "checkpoint_validator_installed_before_v20_forensic_reproduction",
        "failure_reproduced_offline": True,
        "failure_reproduction_result": reproduced,
        "v22_runtime_lock": _record(V22_RUNTIME_LOCK),
        "v22_runtime_lock_report": lock_report,
        "v22_launch_receipt": _record(launch_path),
        "v22_capacity_policy": _record(SOURCE_POLICY),
        "v21_reference_terminal": dict(reference["records"]["terminal"]),
        "v22_output_root": str(V22_OUTPUT_ROOT),
        "v22_output_root_absent": True,
        "semantic_attempt_started": False,
        "thread_started": False,
        "turn_started": False,
        "capacity_checkpoint_count": 0,
        "sidecar_count": 0,
        "semantic_output_count": 0,
        "usage_status": "not_started",
        "usage": None,
        "retry_allowed_in_v22": False,
        "required_next_version": "v23",
        "production_mutated": False,
    }


def build_capacity_policy(
    *,
    failure_audit_path: Path = DEFAULT_FAILURE_AUDIT,
    policy_path: Path = DEFAULT_POLICY,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if failure_audit_path.exists() or policy_path.exists():
        raise CapacityPolicyV23Error("v23 failure audit or policy already exists")
    outer_root = output_root.expanduser().resolve()
    inner_root = outer_root / "fresh-attempt"
    if outer_root.exists() or inner_root.exists():
        raise CapacityPolicyV23Error("v23 calibration root exists before policy freeze")
    source = load_reserve_capacity_policy(SOURCE_POLICY)
    audit = build_v22_presemantic_failure_audit()
    _write_immutable(failure_audit_path, audit)
    policy = {
        key: value
        for key, value in source.items()
        if key
        not in {
            "created_at",
            "phase_id",
            "phase_output_root",
            "semantic_output_root",
            "authorization",
            "recovery",
        }
    }
    policy.update(
        {
            "schema_version": RESERVE_CAPACITY_POLICY_VERSION,
            "created_at": now_iso(),
            "phase_id": "fresh_judge_v5_4_calibration_v23",
            "phase_output_root": str(outer_root),
            "semantic_output_root": str(inner_root),
            "recovery": {
                "schema_version": "pif_app_server_calibration_recovery_binding_v23",
                "source_policy": _record(SOURCE_POLICY),
                "v22_runtime_lock": _record(V22_RUNTIME_LOCK),
                "v22_launch_receipt": dict(audit["v22_launch_receipt"]),
                "v22_failure_audit": _record(failure_audit_path),
                "reference_terminal": dict(audit["v21_reference_terminal"]),
                "reference_validated_before_checkpoint_validator_installation": True,
                "v22_retry_allowed": False,
                "v23_attempt_count": 1,
                "retry_count_per_turn": 0,
                "selection_authorized_before_calibration": False,
                "holdout_authorized": False,
                "production_mutation_allowed": False,
            },
        }
    )
    if policy["ordered_turn_names"] != list(CALIBRATION_TURN_NAMES):
        raise CapacityPolicyV23Error("v23 calibration turn ordering drifted")
    _write_immutable(policy_path, policy)
    return audit, load_reserve_capacity_policy(policy_path)


async def write_launch_receipt(
    *, policy_path: Path, runtime_lock_path: Path, output_path: Path
) -> dict[str, Any]:
    if output_path.exists():
        raise CapacityPolicyV23Error("v23 launch receipt already exists")
    policy = load_reserve_capacity_policy(policy_path)
    outer_root = Path(policy["phase_output_root"]).expanduser().resolve()
    inner_root = Path(policy["semantic_output_root"]).expanduser().resolve()
    if outer_root.exists() or inner_root.exists():
        raise CapacityPolicyV23Error("v23 calibration root exists before launch")
    from .app_server_runtime_lock_v23 import verify_runtime_lock_v23

    lock_report = verify_runtime_lock_v23(
        repo_root=Path.cwd(), manifest_path=runtime_lock_path
    )
    snapshot = await probe_app_server_rate_limits(maximum_primary_used_percent=100)
    if (
        snapshot.get("managed_chatgpt_auth_verified") is not True
        or snapshot.get("plan_type") != "pro"
        or snapshot.get("thread_started") is not False
        or snapshot.get("turn_started") is not False
    ):
        raise CapacityPolicyV23Error("v23 launch probe violated managed auth")
    evaluation = evaluate_reserve_capacity(
        snapshot,
        policy=policy,
        remaining_turn_count=len(policy["ordered_turn_names"]),
    )
    if not evaluation["cleared_for_semantic_turn"]:
        raise CapacityPolicyV23Error("v23 launch does not fit the reserve policy")
    receipt = {
        "schema_version": LAUNCH_RECEIPT_VERSION,
        "created_at": now_iso(),
        "status": "authorized_for_one_fresh_v23_calibration_phase",
        "runtime_lock": _record(runtime_lock_path),
        "runtime_lock_report": lock_report,
        "capacity_policy": _record(policy_path),
        "v22_failure_audit": dict(policy["recovery"]["v22_failure_audit"]),
        "live_capacity": {
            "managed_chatgpt_auth_verified": True,
            "plan_type": snapshot.get("plan_type"),
            "primary_used_percent": snapshot.get("primary_used_percent"),
            "primary_resets_at": snapshot.get("primary_resets_at"),
            "rate_limit_reached_type": snapshot.get("rate_limit_reached_type"),
            "thread_started": False,
            "turn_started": False,
        },
        "reserve_evaluation": evaluation,
        "phase_output_root": str(outer_root),
        "semantic_output_root": str(inner_root),
        "output_roots_absent": True,
        "retry_count_per_turn": 0,
        "selection_authorized_before_calibration": False,
        "holdout_authorized": False,
        "production_mutation_performed": False,
    }
    _write_immutable(output_path, receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare v23 calibration recovery")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--control-root", default=str(DEFAULT_CONTROL_ROOT))
    prepare.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    launch = sub.add_parser("launch-receipt")
    launch.add_argument("--policy", required=True)
    launch.add_argument("--runtime-lock", required=True)
    launch.add_argument("--output", required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    os.environ.pop("OPENAI_API_KEY", None)
    if args.command == "prepare":
        control = Path(args.control_root).expanduser().resolve()
        audit, policy = build_capacity_policy(
            failure_audit_path=control / "presemantic-failure-audit-v22.json",
            policy_path=control / "capacity-policy-v23.json",
            output_root=Path(args.output_root),
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "v22_state": audit["state"],
                    "semantic_attempt_started": audit["semantic_attempt_started"],
                    "phase_id": policy["phase_id"],
                    "projected_quota_points": policy[
                        "projected_phase_quota_points"
                    ],
                },
                sort_keys=True,
            )
        )
        return 0
    receipt = asyncio.run(
        write_launch_receipt(
            policy_path=Path(args.policy),
            runtime_lock_path=Path(args.runtime_lock),
            output_path=Path(args.output),
        )
    )
    print(
        json.dumps(
            {
                "ok": True,
                "status": receipt["status"],
                "primary_used_percent": receipt["live_capacity"][
                    "primary_used_percent"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
