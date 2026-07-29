from __future__ import annotations

"""Measured v20 capacity audit and immutable bounded-phase authorization."""

import argparse
import asyncio
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_capacity_reserve import (
    MINIMUM_REMAINING_RESERVE_PERCENT,
    RESERVE_CAPACITY_POLICY_VERSION,
    evaluate_reserve_capacity,
    load_reserve_capacity_policy,
)
from .app_server_interrupted_arm_recovery import probe_app_server_rate_limits
from .util import now_iso


CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
CAPACITY_LAUNCH_RECEIPT_VERSION = (
    "pif_app_server_reserve_capacity_launch_receipt_v20"
)
DEFAULT_CONTROL_ROOT = Path(
    "work/app-server-development-v2/unattended-control-v20"
).resolve()
DEFAULT_SEMANTIC_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "fixture-reference-adjudication-luna-v5-capacity-v20"
).resolve()
DEFAULT_PRIOR_V2_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "fixture-truth-audit-gpt55-v2"
).resolve()
DEFAULT_PRIOR_V3_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "fixture-truth-audit-gpt55-v3"
).resolve()
DEFAULT_POST_PHASE_SNAPSHOT = Path(
    "work/app-server-development-v2/unattended-control-v16/"
    "blocked-capacity-snapshot.json"
).resolve()
DEFAULT_SELECTION_POOL = Path(
    "work/app-server-development-v2/unattended-pipeline-v1/development-selection/"
    "witness-pool/shared-witness-pool.private.json"
).resolve()

REFERENCE_TURN_NAMES = tuple(
    ["reference_pointwise_shard_%02d" % index for index in range(9)]
    + ["reference_alignment_shard_%02d" % index for index in range(3)]
)


class CapacityPolicyV20Error(RuntimeError):
    """The measured v20 capacity decision cannot be frozen safely."""


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
        raise CapacityPolicyV20Error("capacity evidence artifact is missing")
    return {
        "path": str(target),
        "sha256": _sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityPolicyV20Error("%s is missing or invalid" % purpose) from exc
    if not isinstance(value, dict):
        raise CapacityPolicyV20Error("%s is not an object" % purpose)
    return value


def _write_immutable(path: Path, value: Any) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    content = _canonical_bytes(value)
    try:
        handle = target.open("xb")
    except FileExistsError as exc:
        raise CapacityPolicyV20Error(
            "immutable capacity-policy artifact already exists"
        ) from exc
    with handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def _measured_fixture_rows(
    *, v2_root: Path, v3_root: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    records = []
    for root in (v2_root.expanduser().resolve(), v3_root.expanduser().resolve()):
        for capacity_path in sorted((root / "turns").glob("*/capacity.json")):
            sidecar_path = capacity_path.with_name("sidecar.json")
            capacity = _load(capacity_path, "measured capacity checkpoint")
            sidecar = _load(sidecar_path, "measured usage sidecar")
            usage = sidecar.get("usage")
            total = usage.get("total_tokens") if isinstance(usage, dict) else None
            if (
                capacity.get("managed_chatgpt_auth_verified") is not True
                or capacity.get("rate_limit_reached_type") is not None
                or capacity.get("thread_started") is not False
                or capacity.get("turn_started") is not False
                or sidecar.get("state") != "completed"
                or sidecar.get("status") != "completed"
                or sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
                or isinstance(total, bool)
                or not isinstance(total, int)
                or total <= 0
            ):
                raise CapacityPolicyV20Error("measured fixture evidence drifted")
            custom_bytes = sum(
                int(sidecar.get(field) or 0)
                for field in (
                    "prompt_bytes",
                    "output_schema_bytes",
                    "base_instructions_bytes",
                )
            )
            rows.append(
                {
                    "turn_name": capacity_path.parent.name,
                    "checked_at": capacity.get("checked_at"),
                    "primary_used_percent": capacity.get("primary_used_percent"),
                    "total_tokens": total,
                    "input_tokens": int(usage.get("input_tokens") or 0),
                    "output_tokens": int(usage.get("output_tokens") or 0),
                    "custom_request_bytes": custom_bytes,
                }
            )
            records.extend(
                [
                    {"kind": "capacity", **_record(capacity_path)},
                    {"kind": "sidecar", **_record(sidecar_path)},
                ]
            )
    rows.sort(key=lambda row: str(row["checked_at"]))
    if (
        len(rows) != 24
        or sum(row["total_tokens"] for row in rows) != 854550
        or rows[0]["primary_used_percent"] != 16
    ):
        raise CapacityPolicyV20Error("measured fixture totals drifted")
    return rows, records


def _interval_capacity_rates(
    rows: Sequence[Mapping[str, Any]], *, post_used_percent: int
) -> list[dict[str, Any]]:
    intervals = []
    start = 0
    used = int(rows[0]["primary_used_percent"])
    for index in range(1, len(rows)):
        observed = int(rows[index]["primary_used_percent"])
        if observed < used:
            raise CapacityPolicyV20Error("capacity evidence crosses a reset boundary")
        if observed > used:
            tokens = sum(int(row["total_tokens"]) for row in rows[start:index])
            if tokens <= 0:
                raise CapacityPolicyV20Error("capacity interval has no measured tokens")
            intervals.append(
                {
                    "from_used_percent": used,
                    "to_used_percent": observed,
                    "quota_point_change": observed - used,
                    "measured_tokens": tokens,
                    "quota_points_per_million_tokens": round(
                        (observed - used) * 1_000_000 / tokens, 6
                    ),
                }
            )
            start = index
            used = observed
    if post_used_percent < used:
        raise CapacityPolicyV20Error("post-phase capacity snapshot regressed")
    if post_used_percent > used:
        tokens = sum(int(row["total_tokens"]) for row in rows[start:])
        intervals.append(
            {
                "from_used_percent": used,
                "to_used_percent": post_used_percent,
                "quota_point_change": post_used_percent - used,
                "measured_tokens": tokens,
                "quota_points_per_million_tokens": round(
                    (post_used_percent - used) * 1_000_000 / tokens, 6
                ),
            }
        )
    if not intervals:
        raise CapacityPolicyV20Error("capacity evidence has no measured change")
    return intervals


def _synthetic_size_only_support_receipts(
    pool: Mapping[str, Any]
) -> dict[str, Any]:
    from .app_server_judge_v5 import (
        build_pointwise_support_input,
        freeze_support_receipts,
    )

    pointwise = build_pointwise_support_input(pool)
    rows = []
    for unit in pointwise["units"]:
        source = unit["source_excerpt"]
        span = source[: min(200, len(source))]
        rows.append(
            {
                "case_id": unit["case_id"],
                "witness_id": unit["witness_id"],
                "proposition_verdict": "supported",
                "proposition_evidence_spans": [span],
                "proposition_rationale": "Synthetic receipt used only for request-size accounting.",
                "structured_field_verdict": "correct",
                "field_issue_fields": [],
                "field_evidence_spans": [span],
                "field_rationale": "Synthetic receipt used only for request-size accounting.",
            }
        )
    return freeze_support_receipts({"units": rows}, pointwise)


def _selection_estimate(
    *, pool_path: Path, maximum_turn_tokens: int, quota_rate: int
) -> dict[str, Any]:
    from .app_server_judge_v5_selection import (
        MAX_OUTPUT_SCHEMA_BYTES,
        MAX_PROMPT_BYTES,
        plan_alignment_selection_shards,
        plan_pointwise_selection_shards,
    )

    pool = _load(pool_path, "selection witness pool")
    pointwise = plan_pointwise_selection_shards(pool)
    receipts = _synthetic_size_only_support_receipts(pool)
    case_ids = [str(case["case_id"]) for case in pool["cases"]]
    base = plan_alignment_selection_shards(
        pool=pool,
        support_receipts=receipts,
        case_ids=case_ids,
        permutation="base",
    )
    canary = plan_alignment_selection_shards(
        pool=pool,
        support_receipts=receipts,
        case_ids=case_ids[::4],
        permutation="balanced_canary",
    )
    planned = [*pointwise, *base, *canary]
    planned_custom_bytes = sum(
        int(row["prompt_bytes"]) + int(row["schema_bytes"]) + 1000
        for row in planned
    )
    adjudication_custom_bytes = MAX_PROMPT_BYTES + MAX_OUTPUT_SCHEMA_BYTES + 1000
    turn_count = len(planned) + 1
    custom_bytes = planned_custom_bytes + adjudication_custom_bytes
    # The 24 measured turns all fit input <= 22k + 0.4*custom bytes and
    # total <= 1.75*that input envelope. Keep the same conservative envelope.
    input_bound = turn_count * 22000 + math.ceil(custom_bytes * 0.4)
    total_bound = math.ceil(input_bound * 1.75)
    return {
        "phase": "five_arm_development_selection",
        "authorization_status": "not_authorized_by_v20_reference_policy",
        "case_count": len(case_ids),
        "witness_count": sum(row["witness_count"] for row in pointwise),
        "pointwise_turn_count": len(pointwise),
        "base_alignment_turn_count": len(base),
        "canary_alignment_turn_count": len(canary),
        "adjudication_turn_cap": 1,
        "maximum_turn_count": turn_count,
        "planned_prompt_bytes": sum(int(row["prompt_bytes"]) for row in planned),
        "planned_schema_bytes": sum(int(row["schema_bytes"]) for row in planned),
        "request_size_bound_method": (
            "measured_24_turn_affine_input_envelope_then_1_75_total_multiplier"
        ),
        "conservative_total_token_bound": total_bound,
        "conservative_quota_point_bound": math.ceil(
            total_bound * quota_rate / 1_000_000
        ),
        "single_cycle_fit_at_zero_used_with_20_point_reserve": math.ceil(
            total_bound * quota_rate / 1_000_000
        )
        <= 80,
        "maximum_total_tokens_per_turn_reference": maximum_turn_tokens,
        "semantic_decisions_performed_for_size_estimate": 0,
    }


def build_capacity_audit_and_policy(
    *,
    current_snapshot_path: Path,
    audit_path: Path,
    policy_path: Path,
    semantic_output_root: Path,
    v2_root: Path = DEFAULT_PRIOR_V2_ROOT,
    v3_root: Path = DEFAULT_PRIOR_V3_ROOT,
    post_phase_snapshot_path: Path = DEFAULT_POST_PHASE_SNAPSHOT,
    selection_pool_path: Path = DEFAULT_SELECTION_POOL,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if audit_path.exists() or policy_path.exists():
        raise CapacityPolicyV20Error("v20 audit or policy already exists")
    if semantic_output_root.expanduser().resolve().exists():
        raise CapacityPolicyV20Error("v20 semantic output root is not empty")
    current = _load(current_snapshot_path, "current no-turn capacity snapshot")
    post = _load(post_phase_snapshot_path, "post-fixture capacity snapshot")
    if (
        current.get("managed_chatgpt_auth_verified") is not True
        or current.get("plan_type") != "pro"
        or current.get("rate_limit_reached_type") is not None
        or current.get("thread_started") is not False
        or current.get("turn_started") is not False
        or post.get("managed_chatgpt_auth_verified") is not True
        or post.get("rate_limit_reached_type") is not None
    ):
        raise CapacityPolicyV20Error("capacity snapshot auth or provider state is unsafe")
    rows, evidence_records = _measured_fixture_rows(v2_root=v2_root, v3_root=v3_root)
    post_used = int(post["primary_used_percent"])
    intervals = _interval_capacity_rates(rows, post_used_percent=post_used)
    maximum_interval_rate = max(
        float(row["quota_points_per_million_tokens"]) for row in intervals
    )
    quota_rate = math.ceil(maximum_interval_rate * 1.25)
    measured_max_turn = max(int(row["total_tokens"]) for row in rows)
    maximum_turn_tokens = math.ceil(measured_max_turn * 2 / 1000) * 1000
    phase_total_tokens = maximum_turn_tokens * len(REFERENCE_TURN_NAMES)
    projected_phase_points = math.ceil(
        phase_total_tokens * quota_rate / 1_000_000
    )
    current_used = int(current["primary_used_percent"])
    remaining = 100 - current_used
    usable = remaining - MINIMUM_REMAINING_RESERVE_PERCENT
    selection = _selection_estimate(
        pool_path=selection_pool_path,
        maximum_turn_tokens=maximum_turn_tokens,
        quota_rate=quota_rate,
    )
    calibration_tokens = maximum_turn_tokens * 24
    holdout_max_turns = 1660
    prospective_max_turns = 448
    estimates = [
        {
            "phase": "fixture_reference_adjudication",
            "turn_count": len(REFERENCE_TURN_NAMES),
            "conservative_total_token_bound": phase_total_tokens,
            "conservative_quota_point_bound": projected_phase_points,
            "authorization_status": "authorized_if_live_launch_probe_reconfirms",
        },
        {
            "phase": "fresh_judge_calibration",
            "maximum_turn_count": 24,
            "conservative_total_token_bound": calibration_tokens,
            "conservative_quota_point_bound": math.ceil(
                calibration_tokens * quota_rate / 1_000_000
            ),
            "authorization_status": "requires_new_post_reference_policy",
        },
        selection,
        {
            "phase": "untouched_holdout_stratification_execution_and_judge",
            "known_reservoir_segment_count": 480,
            "selected_evaluation_segment_count": 120,
            "structural_minimum_turn_count": 316,
            "conservative_maximum_turn_count": holdout_max_turns,
            "conservative_total_token_bound": holdout_max_turns
            * maximum_turn_tokens,
            "conservative_quota_point_bound": math.ceil(
                holdout_max_turns
                * maximum_turn_tokens
                * quota_rate
                / 1_000_000
            ),
            "authorization_status": "closed_until_development_winner_is_frozen",
        },
        {
            "phase": "prospective_future_data_gate",
            "minimum_segment_count": 60,
            "structural_minimum_turn_count": 69,
            "conservative_maximum_turn_count": prospective_max_turns,
            "conservative_total_token_bound": prospective_max_turns
            * maximum_turn_tokens,
            "conservative_quota_point_bound": math.ceil(
                prospective_max_turns
                * maximum_turn_tokens
                * quota_rate
                / 1_000_000
            ),
            "authorization_status": "closed_until_holdout_passes_and_future_data_exists",
        },
    ]
    policy = {
        "schema_version": RESERVE_CAPACITY_POLICY_VERSION,
        "created_at": now_iso(),
        "phase_id": "fixture_reference_adjudication_v20",
        "semantic_output_root": str(semantic_output_root.expanduser().resolve()),
        "ordered_turn_names": list(REFERENCE_TURN_NAMES),
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": quota_rate,
        "maximum_total_tokens_per_turn": maximum_turn_tokens,
        "phase_total_token_bound": phase_total_tokens,
        "projected_phase_quota_points": projected_phase_points,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "rate_limit_reached_type_must_be_null": True,
        "reprobe_before_every_semantic_turn": True,
        "execute_turns_serially": True,
        "retry_count_per_turn": 0,
        "unknown_usage_hard_stop": True,
        "auth_drift_hard_stop": True,
        "reserve_loss_hard_stop": True,
        "production_mutation_allowed": False,
        "audit": _record(audit_path) if audit_path.is_file() else None,
    }
    launch_projection = evaluate_reserve_capacity(
        {
            "primary_used_percent": current_used,
            "rate_limit_reached_type": None,
        },
        policy=policy,
        remaining_turn_count=len(REFERENCE_TURN_NAMES),
    )
    if not launch_projection["cleared_for_semantic_turn"]:
        raise CapacityPolicyV20Error("measured reference phase does not fit the reserve")
    audit = {
        "schema_version": CAPACITY_AUDIT_VERSION,
        "created_at": now_iso(),
        "decision": "replace_used_percent_ceiling_with_phase_bound_and_remaining_reserve",
        "legacy_20_percent_used_ceiling_evidence_based": False,
        "reason": (
            "The legacy ceiling constrains used quota without considering phase size. "
            "The measured bounded reference phase fits while retaining at least 20 "
            "percentage points of remaining capacity."
        ),
        "current_no_turn_snapshot": _record(current_snapshot_path),
        "post_fixture_snapshot": _record(post_phase_snapshot_path),
        "measured_fixture_terminals": [
            _record(v2_root / "terminal.json"),
            _record(v3_root / "terminal.json"),
        ],
        "measured_turn_count": len(rows),
        "measured_total_tokens": sum(row["total_tokens"] for row in rows),
        "measured_maximum_turn_tokens": measured_max_turn,
        "measured_start_used_percent": rows[0]["primary_used_percent"],
        "conservative_post_phase_used_percent": post_used,
        "capacity_intervals": intervals,
        "maximum_observed_interval_quota_points_per_million_tokens": round(
            maximum_interval_rate, 6
        ),
        "quota_rate_safety_multiplier": 1.25,
        "frozen_quota_points_per_million_tokens": quota_rate,
        "per_turn_token_safety_multiplier": 2.0,
        "frozen_maximum_total_tokens_per_turn": maximum_turn_tokens,
        "current_primary_used_percent": current_used,
        "current_primary_remaining_percent": remaining,
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "current_usable_percent_above_reserve": usable,
        "reference_projected_quota_points": projected_phase_points,
        "reference_margin_above_reserve_after_projection": usable
        - projected_phase_points,
        "reference_launch_projection": launch_projection,
        "remaining_phase_estimates": estimates,
        "measured_evidence_records": evidence_records,
        "selection_size_estimate_uses_synthetic_receipts_only": True,
        "selection_size_estimate_semantic_evidence": False,
        "production_mutation_performed": False,
    }
    _write_immutable(audit_path, audit)
    policy["audit"] = _record(audit_path)
    _write_immutable(policy_path, policy)
    return audit, load_reserve_capacity_policy(policy_path)


async def probe_immutable_snapshot(output_path: Path) -> dict[str, Any]:
    if output_path.exists():
        return _load(output_path, "existing no-turn capacity snapshot")
    snapshot = await probe_app_server_rate_limits(maximum_primary_used_percent=100)
    _write_immutable(output_path, snapshot)
    return snapshot


async def write_launch_receipt(
    *,
    policy_path: Path,
    runtime_lock_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    if output_path.exists():
        raise CapacityPolicyV20Error("v20 launch receipt already exists")
    policy = load_reserve_capacity_policy(policy_path)
    semantic_root = Path(policy["semantic_output_root"]).expanduser().resolve()
    if semantic_root.exists():
        raise CapacityPolicyV20Error("semantic root exists before v20 launch receipt")
    from .app_server_runtime_lock_v20 import verify_runtime_lock_v20

    lock_report = verify_runtime_lock_v20(
        repo_root=Path.cwd(), manifest_path=runtime_lock_path
    )
    snapshot = await probe_app_server_rate_limits(maximum_primary_used_percent=100)
    if (
        snapshot.get("managed_chatgpt_auth_verified") is not True
        or snapshot.get("plan_type") != "pro"
        or snapshot.get("thread_started") is not False
        or snapshot.get("turn_started") is not False
    ):
        raise CapacityPolicyV20Error("launch probe violated managed-auth boundary")
    evaluation = evaluate_reserve_capacity(
        snapshot,
        policy=policy,
        remaining_turn_count=len(policy["ordered_turn_names"]),
    )
    if not evaluation["cleared_for_semantic_turn"]:
        raise CapacityPolicyV20Error("live launch probe does not fit the reserve policy")
    receipt = {
        "schema_version": CAPACITY_LAUNCH_RECEIPT_VERSION,
        "created_at": now_iso(),
        "status": "authorized_for_one_bounded_reference_phase",
        "runtime_lock": _record(runtime_lock_path),
        "runtime_lock_report": lock_report,
        "capacity_policy": _record(policy_path),
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
        "production_mutation_performed": False,
    }
    _write_immutable(output_path, receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare v20 reserve capacity policy")
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
        snapshot_path = control / "prepolicy-capacity.json"
        audit_path = control / "capacity-policy-audit-v20.json"
        policy_path = control / "capacity-policy-v20.json"
        snapshot = asyncio.run(probe_immutable_snapshot(snapshot_path))
        audit, policy = build_capacity_audit_and_policy(
            current_snapshot_path=snapshot_path,
            audit_path=audit_path,
            policy_path=policy_path,
            semantic_output_root=Path(args.semantic_root),
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "primary_used_percent": snapshot["primary_used_percent"],
                    "quota_points_per_million_tokens": policy[
                        "quota_points_per_million_tokens"
                    ],
                    "reference_projected_quota_points": policy[
                        "projected_phase_quota_points"
                    ],
                    "reference_margin_above_reserve": audit[
                        "reference_margin_above_reserve_after_projection"
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
