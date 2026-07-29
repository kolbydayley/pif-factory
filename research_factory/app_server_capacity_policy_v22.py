from __future__ import annotations

"""Capacity policy and launch authorization for fresh v22 calibration."""

import argparse
import asyncio
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_capacity_reserve import (
    RESERVE_CAPACITY_POLICY_VERSION,
    evaluate_reserve_capacity,
    load_reserve_capacity_policy,
)
from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits
from .app_server_judge_v5_fresh_calibration_v22 import (
    DEFAULT_OUTPUT_ROOT,
    REFERENCE_ROOT,
    load_frozen_fixture_reference_v21,
)
from .util import now_iso


AUTHORIZATION_AUDIT_VERSION = "pif_app_server_calibration_authorization_audit_v22"
LAUNCH_RECEIPT_VERSION = "pif_app_server_calibration_launch_receipt_v22"
DEFAULT_CONTROL_ROOT = Path(
    "work/app-server-development-v2/unattended-control-v22"
).resolve()
DEFAULT_POLICY = DEFAULT_CONTROL_ROOT / "capacity-policy-v22.json"
DEFAULT_AUTHORIZATION_AUDIT = (
    DEFAULT_CONTROL_ROOT / "calibration-authorization-audit-v22.json"
)
SOURCE_POLICY = Path(
    "work/app-server-development-v2/unattended-control-v21/capacity-policy-v21.json"
).resolve()

CALIBRATION_TURN_NAMES = tuple(
    [f"pointwise_support_shard_{index:02d}" for index in range(11)]
    + [f"neutral_alignment_base_shard_{index:02d}" for index in range(11)]
    + ["neutral_alignment_canary", "disagreement_adjudication"]
)


class CapacityPolicyV22Error(RuntimeError):
    """The fresh calibration phase cannot be authorized safely."""


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
        raise CapacityPolicyV22Error("required calibration artifact is missing")
    return {
        "path": str(target),
        "sha256": _sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def _verify_record(record: Any) -> Path:
    if not isinstance(record, Mapping):
        raise CapacityPolicyV22Error("calibration authorization record is malformed")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise CapacityPolicyV22Error("calibration authorization record drifted")
    return path


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityPolicyV22Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise CapacityPolicyV22Error(f"{purpose} is not an object")
    return value


def _write_immutable(path: Path, value: Any) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = target.open("xb")
    except FileExistsError as exc:
        raise CapacityPolicyV22Error("immutable v22 artifact already exists") from exc
    with handle:
        handle.write(_canonical_bytes(value))
        handle.flush()
        os.fsync(handle.fileno())


def build_calibration_authorization_audit() -> dict[str, Any]:
    reference = load_frozen_fixture_reference_v21(REFERENCE_ROOT)
    terminal = reference["terminal"]
    receipt = reference["receipt"]
    if (
        terminal.get("reference_frozen") is not True
        or terminal.get("fresh_calibration_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or receipt.get("fresh_calibration_authorized") is not True
        or receipt.get("selection_authorized") is not False
    ):
        raise CapacityPolicyV22Error("v21 reference does not authorize calibration")
    return {
        "schema_version": AUTHORIZATION_AUDIT_VERSION,
        "created_at": now_iso(),
        "status": "fixture_reference_v2_frozen_fresh_calibration_authorized",
        "reference_records": dict(reference["records"]),
        "reference_turn_count": terminal["turn_count"],
        "reference_new_turn_count": terminal["new_semantic_turn_count"],
        "reference_completed_checkpoint_adoption_count": len(
            terminal["completed_checkpoint_adoptions"]
        ),
        "reference_usage": dict(terminal["usage"]),
        "calibration_model": "gpt-5.6-sol",
        "calibration_reasoning_effort": "high",
        "calibration_case_count": 66,
        "calibration_witness_count": 182,
        "calibration_turn_count_minimum": 23,
        "calibration_turn_count_maximum": 24,
        "ordered_turn_names": list(CALIBRATION_TURN_NAMES),
        "semantic_retry_count_per_turn": 0,
        "prior_calibration_output_reuse_allowed": False,
        "selection_authorized_before_calibration": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def build_capacity_policy(
    *,
    audit_path: Path = DEFAULT_AUTHORIZATION_AUDIT,
    policy_path: Path = DEFAULT_POLICY,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if audit_path.exists() or policy_path.exists():
        raise CapacityPolicyV22Error("v22 authorization audit or policy already exists")
    outer_root = output_root.expanduser().resolve()
    inner_root = outer_root / "fresh-attempt"
    if outer_root.exists() or inner_root.exists():
        raise CapacityPolicyV22Error("v22 calibration root exists before policy freeze")
    source = load_reserve_capacity_policy(SOURCE_POLICY)
    audit = build_calibration_authorization_audit()
    _write_immutable(audit_path, audit)
    maximum = int(source["maximum_total_tokens_per_turn"])
    quota_rate = int(source["quota_points_per_million_tokens"])
    total_bound = len(CALIBRATION_TURN_NAMES) * maximum
    policy = {
        key: value
        for key, value in source.items()
        if key
        not in {
            "created_at",
            "phase_id",
            "semantic_output_root",
            "phase_output_root",
            "ordered_turn_names",
            "phase_total_token_bound",
            "projected_phase_quota_points",
            "recovery",
            "authorization",
        }
    }
    policy.update(
        {
            "schema_version": RESERVE_CAPACITY_POLICY_VERSION,
            "created_at": now_iso(),
            "phase_id": "fresh_judge_v5_4_calibration_v22",
            "phase_output_root": str(outer_root),
            "semantic_output_root": str(inner_root),
            "ordered_turn_names": list(CALIBRATION_TURN_NAMES),
            "phase_total_token_bound": total_bound,
            "projected_phase_quota_points": math.ceil(
                total_bound * quota_rate / 1_000_000
            ),
            "authorization": {
                "schema_version": "pif_app_server_calibration_authorization_binding_v22",
                "source_policy": _record(SOURCE_POLICY),
                "reference_terminal": dict(audit["reference_records"]["terminal"]),
                "reference_receipt": dict(audit["reference_records"]["receipt"]),
                "authorization_audit": _record(audit_path),
                "fresh_calibration_attempt_count": 1,
                "retry_count_per_turn": 0,
                "selection_authorized_before_calibration": False,
                "holdout_authorized": False,
                "production_mutation_allowed": False,
            },
        }
    )
    _write_immutable(policy_path, policy)
    return audit, load_reserve_capacity_policy(policy_path)


async def write_launch_receipt(
    *, policy_path: Path, runtime_lock_path: Path, output_path: Path
) -> dict[str, Any]:
    if output_path.exists():
        raise CapacityPolicyV22Error("v22 launch receipt already exists")
    policy = load_reserve_capacity_policy(policy_path)
    outer_root = Path(policy["phase_output_root"]).expanduser().resolve()
    inner_root = Path(policy["semantic_output_root"]).expanduser().resolve()
    if outer_root.exists() or inner_root.exists():
        raise CapacityPolicyV22Error("v22 calibration root exists before launch")
    from .app_server_runtime_lock_v22 import verify_runtime_lock_v22

    lock_report = verify_runtime_lock_v22(
        repo_root=Path.cwd(), manifest_path=runtime_lock_path
    )
    snapshot = await probe_app_server_rate_limits(maximum_primary_used_percent=100)
    if (
        snapshot.get("managed_chatgpt_auth_verified") is not True
        or snapshot.get("plan_type") != "pro"
        or snapshot.get("thread_started") is not False
        or snapshot.get("turn_started") is not False
    ):
        raise CapacityPolicyV22Error("v22 launch probe violated managed auth")
    evaluation = evaluate_reserve_capacity(
        snapshot,
        policy=policy,
        remaining_turn_count=len(policy["ordered_turn_names"]),
    )
    if not evaluation["cleared_for_semantic_turn"]:
        raise CapacityPolicyV22Error("v22 launch does not fit the reserve policy")
    receipt = {
        "schema_version": LAUNCH_RECEIPT_VERSION,
        "created_at": now_iso(),
        "status": "authorized_for_one_fresh_v22_calibration_phase",
        "runtime_lock": _record(runtime_lock_path),
        "runtime_lock_report": lock_report,
        "capacity_policy": _record(policy_path),
        "authorization_audit": dict(policy["authorization"]["authorization_audit"]),
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
    parser = argparse.ArgumentParser(description="Prepare v22 calibration capacity")
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
            audit_path=control / "calibration-authorization-audit-v22.json",
            policy_path=control / "capacity-policy-v22.json",
            output_root=Path(args.output_root),
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "status": audit["status"],
                    "phase_id": policy["phase_id"],
                    "turn_bound": len(policy["ordered_turn_names"]),
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
