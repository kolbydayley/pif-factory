"""Development-only Gold A/B/C re-authoring after source refreeze.

Planning is available before the refreeze overlay exists. Activation is
fail-closed and consumes the real digest-pinned overlay; it never fabricates
replacement windows and never opens validation or holdout transcript content.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from .signal_desk_gold_repair_v4 import _digest_outputs, _sha_json
from .signal_desk_gold_runner import RESERVE_TOKENS, run_gold_split_phase
from .signal_desk_rebuild_gold import validate_split_manifest, verify_frozen_manifest


SCHEMA_VERSION = "pif_signal_desk_gold_reauthor_plan_v1"
EXPECTED_WINDOWS = 9
EXPECTED_PROVIDER_CALLS = 27
SEMANTIC_MEAN_TOKENS_PER_CALL = 34_540.3
EXPECTED_TOKENS = round(EXPECTED_PROVIDER_CALLS * SEMANTIC_MEAN_TOKENS_PER_CALL)
RESERVED_TOKEN_CEILING = EXPECTED_WINDOWS * sum(RESERVE_TOKENS[p] for p in ("A", "B", "C"))


class ReauthorPlanError(RuntimeError):
    pass


def build_waiting_plan(*, base_manifest_path: Path, refreeze_path: Path, output_root: Path) -> dict[str, Any]:
    base = json.loads(base_manifest_path.read_text())
    verify_frozen_manifest(base)
    plan = {
        "schema_version": SCHEMA_VERSION,
        "scope": "development_replacement_windows_only",
        "base_manifest_sha256": base["manifest_sha256"],
        "required_refreeze_path": str(refreeze_path),
        "required_refreeze_schema": "pif_signal_desk_development_source_refreeze_v1",
        "refreeze_overlay_present": refreeze_path.is_file(),
        "provider_calls_started": False,
        "expected_replacement_windows": EXPECTED_WINDOWS,
        "expected_calls": {"A": 9, "B": 9, "C": 9, "total": EXPECTED_PROVIDER_CALLS},
        "expected_tokens_at_measured_semantic_mean": EXPECTED_TOKENS,
        "reserved_token_ceiling": RESERVED_TOKEN_CEILING,
        "model": "gpt-5.6-sol", "reasoning_effort": "medium",
        "gold_grant_lane": "gpt_5_6_sol_gold_authoring",
        "lease_seconds": 1800, "deadline_seconds": 900,
        "adaptive_concurrency": {"minimum": 2, "maximum": 8, "requested_default": 2,
                                 "reuse_existing_gold_capacity_and_health_guards": True},
        "pipeline_order": ["A", "B", "C"],
        "preservation": {
            "unchanged_development_gold_reused_by_digest": True,
            "validation_and_holdout_outputs_read_or_written": False,
            "sealed_content_opened": False,
            "base_manifest_mutated": False,
        },
        "output_root": str(output_root),
    }
    plan["plan_sha256"] = _sha_json(plan)
    return plan


def validate_overlay(overlay: Mapping[str, Any], *, base_manifest_sha256: str) -> list[dict[str, Any]]:
    if overlay.get("schema_version") != "pif_signal_desk_development_source_refreeze_v1":
        raise ReauthorPlanError("unexpected refreeze schema")
    if overlay.get("base_manifest_sha256") != base_manifest_sha256:
        raise ReauthorPlanError("refreeze overlay is bound to a different base manifest")
    if overlay.get("base_manifest_mutated") is not False or overlay.get("validation_or_holdout_content_opened") is not False:
        raise ReauthorPlanError("refreeze violated base/sealed isolation")
    if overlay.get("ready_for_development_gold_authoring") is not True:
        raise ReauthorPlanError("refreeze is not ready for development Gold")
    rows = list(overlay.get("replacement_windows") or [])
    if len(rows) != EXPECTED_WINDOWS or int(overlay.get("replacement_window_count") or 0) != EXPECTED_WINDOWS:
        raise ReauthorPlanError("refreeze must contain exactly nine replacement windows")
    if any(row.get("split") != "development" for row in rows):
        raise ReauthorPlanError("refreeze attempted to replace a non-development window")
    if len({str(row["window_id"]) for row in rows}) != EXPECTED_WINDOWS:
        raise ReauthorPlanError("replacement window IDs are not unique")
    if _sha_json(sorted(rows, key=lambda row: row["window_id"])) != overlay.get("replacement_windows_digest"):
        raise ReauthorPlanError("replacement-window digest mismatch")
    return rows


def activate_overlay(
    *, base_manifest_path: Path, refreeze_path: Path, project_root: Path,
    merged_manifest_path: Path, waiting_plan: Mapping[str, Any],
) -> dict[str, Any]:
    if not refreeze_path.is_file(): raise ReauthorPlanError("real refreeze overlay does not exist")
    base=json.loads(base_manifest_path.read_text());verify_frozen_manifest(base)
    if waiting_plan["base_manifest_sha256"] != base["manifest_sha256"]:raise ReauthorPlanError("base manifest changed after planning")
    overlay=json.loads(refreeze_path.read_text());replacement=validate_overlay(overlay,base_manifest_sha256=base["manifest_sha256"])
    removed=set(str(value) for value in overlay["removed_development_window_ids"])
    if len(removed)!=EXPECTED_WINDOWS:raise ReauthorPlanError("exactly nine old development windows must be removed")
    old={str(row["window_id"]):row for row in base["windows"]}
    if not removed <= set(old) or any(old[value]["split"]!="development" for value in removed):raise ReauthorPlanError("removed IDs are not development windows")
    # Verify only the new development bytes. Protected split transcript paths
    # are never opened by this activation path.
    for row in replacement:
        path=Path(str(row["transcript_path"]));path=path if path.is_absolute() else project_root/path
        text=path.read_text();digest=hashlib.sha256(text.encode()).hexdigest()
        if digest!=row["transcript_sha256"]:raise ReauthorPlanError("replacement transcript digest mismatch")
        window=text[int(row["start_char"]):int(row["end_char"])]
        if hashlib.sha256(window.encode()).hexdigest()!=row["text_sha256"]:raise ReauthorPlanError("replacement window digest mismatch")
    windows=sorted([row for row in base["windows"] if str(row["window_id"]) not in removed]+replacement,key=lambda row:str(row["window_id"]))
    merged={key:json.loads(json.dumps(value)) for key,value in base.items() if key!="manifest_sha256"};merged["windows"]=windows
    split_counts={};structure_counts={}
    for row in windows:
        split_counts[row["split"]]=split_counts.get(row["split"],0)+1;structure_counts[row["transcript_structure"]]=structure_counts.get(row["transcript_structure"],0)+1
    merged["counts"]["windows"]=len(windows);merged["counts"]["episodes"]=len({row["episode_id"] for row in windows});merged["counts"]["by_split"]=dict(sorted(split_counts.items()));merged["counts"]["by_transcript_structure"]=dict(sorted(structure_counts.items()))
    validate_split_manifest(merged);merged["manifest_sha256"]=_sha_json(merged);verify_frozen_manifest(merged)
    merged_manifest_path.parent.mkdir(parents=True,exist_ok=True);merged_manifest_path.write_text(json.dumps(merged,indent=2,sort_keys=True)+"\n")
    protected=[row for row in windows if row["split"] in {"validation","sealed_holdout"}]
    if _sha_json(protected)!=overlay["validation_and_holdout_metadata_digest"]:raise ReauthorPlanError("protected split metadata changed")
    receipt={"schema_version":"pif_signal_desk_gold_reauthor_activation_v1","waiting_plan_sha256":waiting_plan["plan_sha256"],"refreeze_receipt_sha256":overlay["receipt_sha256"],"merged_manifest_sha256":merged["manifest_sha256"],"target_window_ids":sorted(str(row["window_id"]) for row in replacement),"target_window_ids_sha256":_sha_json(sorted(str(row["window_id"]) for row in replacement)),"target_window_count":EXPECTED_WINDOWS,"protected_metadata_digest":_sha_json(protected),"protected_content_opened":False,"provider_calls_started":0}
    receipt["receipt_sha256"]=_sha_json(receipt);return receipt


def merge_development_gold(*, base_root: Path, replacement_root: Path, merged_root: Path, base_manifest_path: Path, merged_manifest_path: Path, target_ids: Sequence[str]) -> dict[str, Any]:
    base=json.loads(base_manifest_path.read_text());merged=json.loads(merged_manifest_path.read_text());removed=set(str(row["window_id"]) for row in base["windows"] if row["split"]=="development")-set(str(row["window_id"]) for row in merged["windows"] if row["split"]=="development")
    target=set(target_ids);unchanged=[str(row["window_id"]) for row in merged["windows"] if row["split"]=="development" and str(row["window_id"]) not in target]
    records=[]
    for turn in ("A","B","C"):
        destination=merged_root/turn;destination.mkdir(parents=True,exist_ok=True)
        for window_id,source in [(value,base_root/turn/f"{value}.json") for value in unchanged]+[(value,replacement_root/turn/f"{value}.json") for value in sorted(target)]:
            if not source.is_file():raise ReauthorPlanError(f"missing Gold {turn} output")
            target_path=destination/source.name
            if not target_path.exists():os.link(source,target_path)
            if target_path.read_bytes()!=source.read_bytes():raise ReauthorPlanError("merged Gold artifact conflict")
            records.append((turn,window_id,hashlib.sha256(source.read_bytes()).hexdigest()))
    receipt={"schema_version":"pif_signal_desk_merged_development_gold_v1","development_window_count":len(unchanged)+len(target),"unchanged_window_count":len(unchanged),"replacement_window_count":len(target),"removed_old_window_count":len(removed),"output_count":len(records),"outputs_digest":_sha_json(records),"validation_or_holdout_outputs_touched":False,"complete":len(records)==189*3}
    receipt["receipt_sha256"]=_sha_json(receipt);return receipt


def readiness_receipt(*, merged_manifest_sha256: str, merged_gold_receipt: Mapping[str, Any]) -> dict[str, Any]:
    receipt={"schema_version":"pif_signal_desk_corrected_dev_gate_readiness_v1","merged_manifest_sha256":merged_manifest_sha256,"merged_gold_receipt_sha256":merged_gold_receipt["receipt_sha256"],"dev_blind_audit":{"status":"ready_to_run","must_reselect_deterministically_on_corrected_manifest":True,"old_dev_audit_invalidated":True},"a1":{"status":"blocked_pending_corrected_dev_audit_pass","old_ceiling_invalidated":True},"a2":{"status":"blocked_pending_corrected_a1_and_scorer_sample","scorer_version":"v5"},"validation_holdout_gold_unchanged":True,"provider_calls_started_by_receipt":0}
    receipt["receipt_sha256"]=_sha_json(receipt);return receipt


async def execute_reauthor(*, merged_manifest_path: Path, target_ids: Sequence[str], project_root: Path, result_root: Path, dispatch_database: Path, budget_database: Path, grant_path: Path, session_root: Path, budget_dir: Path, concurrency: int=2, binary: str="codex") -> dict[str, Any]:
    return await run_gold_split_phase(manifest_path=merged_manifest_path,project_root=project_root,result_root=result_root,dispatch_database=dispatch_database,budget_database=budget_database,grant_path=grant_path,session_root=session_root,budget_dir=budget_dir,split="development",turn_type="PIPELINE",task_namespace="dev-source-remediation-v1",concurrency=concurrency,binary=binary,target_window_ids=target_ids)
