"""Fail-closed orchestration contract for rebuild gold authoring and A1/A2.

This module plans and validates the paid authoring campaign. It deliberately
keeps item-level validation, holdout, and blind-audit material out of readable
development paths. Remote execution is resumable and may start only after the
plan receipt passes all invariants.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .signal_desk_rebuild_gold import (
    build_gold_packets,
    select_blind_gold_audit_windows,
    verify_frozen_manifest,
)


SCHEMA_VERSION = "pif_signal_desk_rebuild_gold_authoring_plan_v1"
GOLD_MODEL = "gpt-5.6-sol"
GOLD_EFFORT = "medium"
GOLD_PASSES = ("A", "B", "C")
MIN_CONCURRENCY = 4
MAX_CONCURRENCY = 8
GPT55_GRANT_MODEL = "gpt-5.5"


class GoldAuthoringPlanError(RuntimeError):
    pass


def _sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def storage_class(split: str, *, blind_audit: bool = False) -> str:
    if blind_audit or split in {"validation", "sealed_holdout"}:
        return "sealed_item_storage"
    if split == "development":
        return "development_item_storage"
    raise GoldAuthoringPlanError(f"unknown benchmark split: {split}")


def build_authoring_plan(
    manifest: Mapping[str, Any],
    *,
    concurrency: int = 4,
) -> dict[str, Any]:
    """Build the one-sequence H1/H2 plan without opening item answers."""

    verify_frozen_manifest(manifest)
    if not manifest.get("complete_benchmark") or len(manifest["windows"]) != 804:
        raise GoldAuthoringPlanError("gold authoring requires the complete frozen 804")
    if not MIN_CONCURRENCY <= concurrency <= MAX_CONCURRENCY:
        raise GoldAuthoringPlanError("gold concurrency must be between 4 and 8")

    split_counts: dict[str, int] = {}
    for window in manifest["windows"]:
        split = str(window["split"])
        split_counts[split] = split_counts.get(split, 0) + 1
    audit_ids = select_blind_gold_audit_windows(manifest)
    gold_calls = len(manifest["windows"]) * len(GOLD_PASSES) + len(audit_ids)
    dev_calls = split_counts.get("development", 0)
    scorer_cases = min(100, dev_calls)
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "manifest_sha256": manifest["manifest_sha256"],
        "blocking": True,
        "model": GOLD_MODEL,
        "reasoning_effort": GOLD_EFFORT,
        "concurrency": concurrency,
        "sequence": [
            "gold_a_all_splits",
            "gold_b_all_splits",
            "gold_c_all_splits",
            "blind_reliability_audit",
            "single_pass_development_calibration",
            "scorer_qualification_from_calibration_pairs",
        ],
        "calls": {
            "gold_a": len(manifest["windows"]),
            "gold_b": len(manifest["windows"]),
            "gold_c": len(manifest["windows"]),
            "blind_audit": len(audit_ids),
            "gold_total": gold_calls,
            "a1_single_pass_development": dev_calls,
            "a2_adjudicated_scorer_decisions": scorer_cases,
            "campaign_total_maximum": gold_calls + dev_calls + scorer_cases,
        },
        "blind_audit": {
            "fraction": 0.10,
            "window_count": len(audit_ids),
            "window_ids_sha256": _sha(list(audit_ids)),
            "item_outputs": "sealed",
        },
        "storage": {
            "development": storage_class("development"),
            "validation": storage_class("validation"),
            "sealed_holdout": storage_class("sealed_holdout"),
            "blind_audit": storage_class("development", blind_audit=True),
            "sealed_reporting": "aggregate_only_from_authoring_time",
        },
        "a1_a2_shared_run": {
            "prediction_run_count": 1,
            "prediction_split": "development",
            "frontier_ceiling_input": "all_development_prediction_gold_pairs",
            "scorer_sample_input": "same_development_prediction_gold_pairs",
            "scorer_sample": "100_stratified_match_no_match_decisions",
            "additional_prediction_run_allowed": False,
        },
        "budget": {
            "lane": "gpt_5_6_sol_gold_authoring",
            "gpt_5_5_rebuild_grant_model": GPT55_GRANT_MODEL,
            "eligible_for_gpt_5_5_rebuild_grant": False,
            "attribution_invariant": "model_and_lane_are_recorded_per_call",
        },
    }
    payload["plan_sha256"] = _sha(payload)
    return payload


def verify_authoring_plan(plan: Mapping[str, Any]) -> None:
    expected = str(plan.get("plan_sha256") or "")
    body = {key: value for key, value in plan.items() if key != "plan_sha256"}
    if plan.get("schema_version") != SCHEMA_VERSION or not expected or _sha(body) != expected:
        raise GoldAuthoringPlanError("gold authoring plan hash verification failed")
    if plan.get("model") != GOLD_MODEL:
        raise GoldAuthoringPlanError("gold authoring model drifted")
    if (plan.get("budget") or {}).get("eligible_for_gpt_5_5_rebuild_grant") is not False:
        raise GoldAuthoringPlanError("GPT-5.5 rebuild grant leaked into gold authoring")
    storage = plan.get("storage") or {}
    if any(storage.get(split) != "sealed_item_storage" for split in ("validation", "sealed_holdout", "blind_audit")):
        raise GoldAuthoringPlanError("sealed item output routed to a readable surface")


def write_authoring_plan(path: Path, plan: Mapping[str, Any]) -> None:
    verify_authoring_plan(plan)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def materialize_authoring_inputs(
    manifest: Mapping[str, Any],
    *,
    project_root: Path,
    output_root: Path,
) -> dict[str, Any]:
    """Write transcript-bearing packets to their split-correct private roots."""

    plan = build_authoring_plan(manifest)
    audit_ids = set(select_blind_gold_audit_windows(manifest))
    counts: dict[str, int] = {}
    for gold_pass in GOLD_PASSES:
        packets = build_gold_packets(
            manifest,
            project_root=project_root,
            gold_pass=gold_pass,
            splits=("development", "validation", "sealed_holdout"),
            allow_sealed=True,
        )
        for packet in packets:
            split = str(packet["input"]["split"])
            window_id = str(packet["input"]["window_id"])
            storage = storage_class(split)
            path = output_root / storage / split / gold_pass / f"{window_id}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            if storage == "sealed_item_storage":
                path.parent.chmod(0o700)
            path.write_text(json.dumps(packet, sort_keys=True) + "\n", encoding="utf-8")
            path.chmod(0o600)
            key = f"{storage}:{split}:{gold_pass}"
            counts[key] = counts.get(key, 0) + 1

    # The audit packet is an independent fourth view of the same frozen text.
    # It always lives under sealed storage, including development members.
    a_packets = build_gold_packets(
        manifest,
        project_root=project_root,
        gold_pass="A",
        splits=("development", "validation", "sealed_holdout"),
        allow_sealed=True,
    )
    for packet in a_packets:
        window_id = str(packet["input"]["window_id"])
        if window_id not in audit_ids:
            continue
        audit_packet = {**packet, "gold_pass": "AUDIT"}
        path = output_root / "sealed_item_storage" / "blind_audit" / f"{window_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.parent.chmod(0o700)
        path.write_text(json.dumps(audit_packet, sort_keys=True) + "\n", encoding="utf-8")
        path.chmod(0o600)
    counts["sealed_item_storage:blind_audit"] = len(audit_ids)

    receipt: dict[str, Any] = {
        "schema_version": "pif_signal_desk_rebuild_gold_input_receipt_v1",
        "manifest_sha256": manifest["manifest_sha256"],
        "plan_sha256": plan["plan_sha256"],
        "counts": dict(sorted(counts.items())),
        "total_model_calls": plan["calls"]["gold_total"],
        "blind_audit_window_ids_sha256": plan["blind_audit"]["window_ids_sha256"],
        "contains_transcript_text": False,
        "sealed_item_outputs": True,
        "paid_calls_started": False,
    }
    receipt["receipt_sha256"] = _sha(receipt)
    return receipt
