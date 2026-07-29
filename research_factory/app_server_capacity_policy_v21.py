from __future__ import annotations

"""Versioned recovery policy for the v20 reserve-checkpoint validator failure."""

import argparse
import asyncio
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_diagnostic as diagnostic
from .app_server_capacity_reserve import (
    RESERVE_CAPACITY_POLICY_VERSION,
    evaluate_reserve_capacity,
    load_reserve_capacity_policy,
)
from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits
from .app_server_judge_v5 import (
    pointwise_support_base_instructions,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_reference_adjudication_v21 import (
    build_reserve_capacity_checkpoint_validator,
)
from .util import now_iso


RECOVERY_AUDIT_VERSION = "pif_app_server_reference_recovery_audit_v21"
RECOVERY_LAUNCH_RECEIPT_VERSION = (
    "pif_app_server_reserve_capacity_launch_receipt_v21"
)
DEFAULT_CONTROL_ROOT = Path(
    "work/app-server-development-v2/unattended-control-v21"
).resolve()
DEFAULT_SEMANTIC_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "fixture-reference-adjudication-luna-v6-capacity-v21"
).resolve()
DEFAULT_V20_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "fixture-reference-adjudication-luna-v5-capacity-v20"
).resolve()
DEFAULT_SOURCE_POLICY = Path(
    "work/app-server-development-v2/unattended-control-v20/capacity-policy-v20.json"
).resolve()


class CapacityPolicyV21Error(RuntimeError):
    """The v21 recovery cannot be authorized safely."""


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
        raise CapacityPolicyV21Error("required recovery artifact is missing")
    return {
        "path": str(target),
        "sha256": _sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def _verify_record(record: Any) -> Path:
    if not isinstance(record, Mapping):
        raise CapacityPolicyV21Error("recovery artifact record is malformed")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise CapacityPolicyV21Error("recovery artifact record drifted")
    return path


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityPolicyV21Error("%s is missing or invalid" % purpose) from exc
    if not isinstance(value, dict):
        raise CapacityPolicyV21Error("%s is not an object" % purpose)
    return value


def _write_immutable(path: Path, value: Any) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = target.open("xb")
    except FileExistsError as exc:
        raise CapacityPolicyV21Error("immutable v21 artifact already exists") from exc
    with handle:
        handle.write(_canonical_bytes(value))
        handle.flush()
        os.fsync(handle.fileno())


def audit_v20_checkpoint_validator_failure(
    *,
    v20_root: Path = DEFAULT_V20_ROOT,
    source_policy_path: Path = DEFAULT_SOURCE_POLICY,
) -> dict[str, Any]:
    root = v20_root.expanduser().resolve()
    source_policy = source_policy_path.expanduser().resolve()
    terminal_path = root / "terminal.json"
    terminal = _load(terminal_path, "v20 terminal")
    turn_root = root / "turns/reference-pointwise-shard-00"
    paths = {
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason")
        != "infrastructure_or_judge_attempt_failed"
        or terminal.get("error_class") != "JudgeV5DiagnosticError"
        or terminal.get("failed_turn_name") != "reference_pointwise_shard_00"
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("production_mutated") is not False
        or terminal.get("reference_frozen") is not False
        or terminal.get("fresh_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
    ):
        raise CapacityPolicyV21Error("v20 terminal contract drifted")
    semantic_files = {
        "capacity": list((root / "turns").glob("*/capacity.json")),
        "sidecar": list((root / "turns").glob("*/sidecar.json")),
        "output": list((root / "turns").glob("*/output.private.json")),
    }
    if any(len(rows) != 1 or rows[0] != paths[kind] for kind, rows in semantic_files.items()):
        raise CapacityPolicyV21Error("v20 semantic-attempt count drifted")
    pointwise_input = _load(paths["input"], "v20 pointwise input")
    output = _load(paths["output"], "v20 pointwise output")
    semantic_errors = list(validate_pointwise_support_output(output, pointwise_input))
    if semantic_errors:
        raise CapacityPolicyV21Error("v20 semantic output is not reusable evidence")
    sidecar = _load(paths["sidecar"], "v20 sidecar")
    usage = diagnostic._validate_usage(sidecar)
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != "gpt-5.6-luna"
        or sidecar.get("effort") != "high"
        or sidecar.get("production_mutated") not in {None, False}
    ):
        raise CapacityPolicyV21Error("v20 sidecar contract drifted")
    prompt = paths["prompt"].read_text(encoding="utf-8")
    schema = _load(paths["schema"], "v20 output schema")
    legacy_error = None
    try:
        diagnostic._validate_completed_turn(
            paths=paths,
            prompt=prompt,
            schema=schema,
            base_instructions=pointwise_support_base_instructions(),
            model="gpt-5.6-luna",
            reasoning_effort="high",
            output_validator=lambda value: validate_pointwise_support_output(
                value, pointwise_input
            ),
        )
    except diagnostic.JudgeV5DiagnosticError as exc:
        legacy_error = str(exc)
    if legacy_error != "semantic turn capacity checkpoint is invalid":
        raise CapacityPolicyV21Error("v20 local validator failure did not reproduce")
    original_validator = diagnostic._validate_capacity_checkpoint
    diagnostic._validate_capacity_checkpoint = (
        build_reserve_capacity_checkpoint_validator(source_policy)
    )
    try:
        diagnostic._validate_completed_turn(
            paths=paths,
            prompt=prompt,
            schema=schema,
            base_instructions=pointwise_support_base_instructions(),
            model="gpt-5.6-luna",
            reasoning_effort="high",
            output_validator=lambda value: validate_pointwise_support_output(
                value, pointwise_input
            ),
        )
    except Exception as exc:
        raise CapacityPolicyV21Error(
            "v21 validator did not clear the completed v20 turn"
        ) from exc
    finally:
        diagnostic._validate_capacity_checkpoint = original_validator
    attempts = terminal.get("attempts")
    completed_attempts = [
        row
        for row in attempts or []
        if isinstance(row, dict) and row.get("state") == "completed"
    ]
    if (
        len(completed_attempts) != 1
        or completed_attempts[0].get("turn_name")
        != "reference_pointwise_shard_00"
    ):
        raise CapacityPolicyV21Error("v20 completed attempt coverage drifted")
    for kind in ("capacity", "sidecar", "output"):
        _verify_record(completed_attempts[0].get(kind))
    return {
        "schema_version": RECOVERY_AUDIT_VERSION,
        "created_at": now_iso(),
        "classification": "repairable_local_checkpoint_validator_schema_mismatch",
        "v20_terminal": _record(terminal_path),
        "v20_reference_spec": _record(root / "reference-adjudication-spec.json"),
        "failed_turn": "reference_pointwise_shard_00",
        "completed_semantic_turn_count": 1,
        "semantic_retry_count": 0,
        "capacity_cleared": True,
        "transport_completed": True,
        "managed_chatgpt_auth_verified": True,
        "semantic_output_validator_error_count": 0,
        "legacy_failure_reproduced": legacy_error,
        "reserve_validator_accepts_completed_turn": True,
        "usage_status": "complete",
        "usage": usage,
        "turn_artifacts": {kind: _record(path) for kind, path in paths.items()},
        "required_next_artifact_path": str(
            (DEFAULT_CONTROL_ROOT / "capacity-policy-v21.json").resolve()
        ),
        "v20_replay_allowed": False,
        "v21_semantic_attempt_started": False,
        "production_mutated": False,
    }


def build_recovery_policy(
    *,
    audit_path: Path,
    policy_path: Path,
    semantic_output_root: Path = DEFAULT_SEMANTIC_ROOT,
    source_policy_path: Path = DEFAULT_SOURCE_POLICY,
    v20_root: Path = DEFAULT_V20_ROOT,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if audit_path.exists() or policy_path.exists():
        raise CapacityPolicyV21Error("v21 audit or policy already exists")
    semantic_root = semantic_output_root.expanduser().resolve()
    if semantic_root.exists():
        raise CapacityPolicyV21Error("v21 semantic root exists before policy freeze")
    source_policy_file = source_policy_path.expanduser().resolve()
    source = load_reserve_capacity_policy(source_policy_file)
    source_turn_names = list(source["ordered_turn_names"])
    adopted_turn_name = "reference_pointwise_shard_00"
    if source_turn_names[0] != adopted_turn_name:
        raise CapacityPolicyV21Error("v20 completed-turn ordering drifted")
    remaining_turn_names = source_turn_names[1:]
    if not remaining_turn_names:
        raise CapacityPolicyV21Error("v21 recovery has no remaining turns")
    audit = audit_v20_checkpoint_validator_failure(
        v20_root=v20_root, source_policy_path=source_policy_file
    )
    _write_immutable(audit_path, audit)
    policy = {
        key: value
        for key, value in source.items()
        if key not in {"created_at", "phase_id", "semantic_output_root", "recovery"}
    }
    policy.update(
        {
            "schema_version": RESERVE_CAPACITY_POLICY_VERSION,
            "created_at": now_iso(),
            "phase_id": "fixture_reference_adjudication_v21",
            "semantic_output_root": str(semantic_root),
            "ordered_turn_names": remaining_turn_names,
            "phase_total_token_bound": len(remaining_turn_names)
            * int(source["maximum_total_tokens_per_turn"]),
            "projected_phase_quota_points": math.ceil(
                len(remaining_turn_names)
                * int(source["maximum_total_tokens_per_turn"])
                * int(source["quota_points_per_million_tokens"])
                / 1_000_000
            ),
            "recovery": {
                "schema_version": "pif_app_server_reference_recovery_binding_v21",
                "source_policy": _record(source_policy_file),
                "v20_terminal": _record(v20_root / "terminal.json"),
                "recovery_audit": _record(audit_path),
                "adopted_completed_turn": {
                    "turn_name": adopted_turn_name,
                    "source_phase_id": source["phase_id"],
                    "source_artifacts": dict(audit["turn_artifacts"]),
                    "usage": dict(audit["usage"]),
                    "semantic_output_validated": True,
                    "capacity_checkpoint_validated": True,
                    "adoption_is_not_retry": True,
                },
                "v20_replay_allowed": False,
                "semantic_attempt_count_for_v21": 1,
                "semantic_turn_attempt_count_for_v21": len(remaining_turn_names),
                "full_reference_turn_count": len(source_turn_names),
            },
        }
    )
    _write_immutable(policy_path, policy)
    return audit, load_reserve_capacity_policy(policy_path)


async def write_launch_receipt(
    *, policy_path: Path, runtime_lock_path: Path, output_path: Path
) -> dict[str, Any]:
    if output_path.exists():
        raise CapacityPolicyV21Error("v21 launch receipt already exists")
    policy = load_reserve_capacity_policy(policy_path)
    semantic_root = Path(policy["semantic_output_root"]).expanduser().resolve()
    if semantic_root.exists():
        raise CapacityPolicyV21Error("v21 semantic root exists before launch receipt")
    from .app_server_runtime_lock_v21 import verify_runtime_lock_v21

    lock_report = verify_runtime_lock_v21(
        repo_root=Path.cwd(), manifest_path=runtime_lock_path
    )
    snapshot = await probe_app_server_rate_limits(maximum_primary_used_percent=100)
    if (
        snapshot.get("managed_chatgpt_auth_verified") is not True
        or snapshot.get("plan_type") != "pro"
        or snapshot.get("thread_started") is not False
        or snapshot.get("turn_started") is not False
    ):
        raise CapacityPolicyV21Error("v21 launch probe violated managed-auth boundary")
    evaluation = evaluate_reserve_capacity(
        snapshot,
        policy=policy,
        remaining_turn_count=len(policy["ordered_turn_names"]),
    )
    if not evaluation["cleared_for_semantic_turn"]:
        raise CapacityPolicyV21Error("v21 launch probe does not fit the reserve policy")
    receipt = {
        "schema_version": RECOVERY_LAUNCH_RECEIPT_VERSION,
        "created_at": now_iso(),
        "status": "authorized_for_one_fresh_v21_reference_phase",
        "runtime_lock": _record(runtime_lock_path),
        "runtime_lock_report": lock_report,
        "capacity_policy": _record(policy_path),
        "recovery_audit": dict(policy["recovery"]["recovery_audit"]),
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
        "semantic_output_root": str(semantic_root),
        "semantic_output_root_absent": True,
        "retry_count_per_turn": 0,
        "v20_replay_allowed": False,
        "production_mutation_performed": False,
    }
    _write_immutable(output_path, receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare v21 reference recovery")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--control-root", default=str(DEFAULT_CONTROL_ROOT))
    prepare.add_argument("--semantic-root", default=str(DEFAULT_SEMANTIC_ROOT))
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
        audit, policy = build_recovery_policy(
            audit_path=control / "recovery-audit-v21.json",
            policy_path=control / "capacity-policy-v21.json",
            semantic_output_root=Path(args.semantic_root),
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "classification": audit["classification"],
                    "v20_total_tokens": audit["usage"]["total_tokens"],
                    "v21_phase_id": policy["phase_id"],
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
