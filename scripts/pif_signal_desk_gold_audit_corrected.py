#!/usr/bin/env python3
"""Fresh corrected-source dev audit, preserving quarantines in its denominator."""
from __future__ import annotations
import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research_factory.signal_desk_gold_runner import _audit_target_ids, run_gold_split_phase
from research_factory.signal_desk_rebuild_gold import build_gold_packets
from research_factory.signal_desk_gold_audit import evaluate_dev_audit
from research_factory.signal_desk_gold_repair_v4 import _sha_json
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json

G = ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
R = G / "development-source-reauthor-v1"
PROJECTION = R / "provenance-merged-development-v1"
OUT = R / "fresh-dev-audit-v1"
MANIFEST = R / "merged-manifest.json"


def retain_quarantined_candidates(projected, raw, expected_missing):
    """Missing claims remain audit candidates; this does not publish/approve them."""
    kept = {e["event_id"]: e for e in projected["events"]}
    missing = [e for e in raw["events"] if e["event_id"] not in kept]
    if len(missing) != expected_missing:
        raise ValueError("quarantine count changed")
    if set(kept) - {e["event_id"] for e in raw["events"]}:
        raise ValueError("projection invented an event")
    return {**projected, "window_disposition": raw["window_disposition"],
            "events": [kept.get(e["event_id"], e) for e in raw["events"]]}


def prepare():
    manifest = json.loads(MANIFEST.read_text())
    merge_receipt = json.loads((PROJECTION / "receipt.json").read_text())
    if merge_receipt["merged_manifest_sha256"] != manifest["manifest_sha256"]:
        raise ValueError("projection manifest mismatch")
    if _sha_json(merge_receipt["artifacts"]) != merge_receipt["artifacts_digest"]:
        raise ValueError("projection inventory digest mismatch")
    inventory = {r["window_id"]: r for r in merge_receipt["artifacts"]}
    residual = {r["window_id"]: r["quarantined_events"] for r in merge_receipt["residual_quarantines"]}
    packets = build_gold_packets(manifest, project_root=ROOT, gold_pass="A", splits=("development",))
    by_window = {p["input"]["window_id"]: p for p in packets}
    if set(by_window) != set(inventory):
        raise ValueError("projection window inventory mismatch")
    records = []
    for wid in sorted(by_window):
        candidate = json.loads((PROJECTION / "C" / f"{wid}.json").read_text())
        if _sha_json(candidate) != inventory[wid]["c_sha256"]:
            raise ValueError("projection C digest mismatch")
        raw = json.loads((R / "merged-development/C" / f"{wid}.json").read_text())
        value = retain_quarantined_candidates(candidate, raw, residual.get(wid, 0))
        immutable_json(OUT / "results/C" / f"{wid}.json", value)
        records.append({"window_id": wid, "c_sha256": _sha_json(value)})
    targets, power, frozen, atomicity = _audit_target_ids(
        manifest=manifest, split="development", by_window=by_window, result_root=OUT / "results")
    plan = {"schema_version": "pif_corrected_dev_audit_plan_v1", "manifest_sha256": manifest["manifest_sha256"],
            "projection_digest": merge_receipt["artifacts_digest"], "audit_candidate_digest": _sha_json(records),
            "initial_windows": len(frozen), "target_windows": len(targets), "target_ids": targets,
            "initial_ids": list(frozen), "atomicity_ids": list(atomicity), "power": power,
            "quarantined_candidate_count_retained": sum(residual.values()),
            "quarantined_window_ids": sorted(residual), "old_audit_or_adjudications_reused": False,
            "gold_accepted": False, "supplemental_residual_review_required": True}
    immutable_json(OUT / "plan.json", plan)
    return plan


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--concurrency", type=int, choices=range(2, 9), default=2)
    a = p.parse_args()
    plan = prepare()
    print(json.dumps({k: plan[k] for k in ("initial_windows", "target_windows", "power", "quarantined_candidate_count_retained", "gold_accepted")}), flush=True)
    if not a.execute:
        return 0
    run = asyncio.run(run_gold_split_phase(
        manifest_path=MANIFEST, project_root=ROOT, result_root=OUT / "results",
        dispatch_database=OUT / "dispatch.sqlite", budget_database=ROOT / "data/factory.sqlite",
        grant_path=ROOT / "config/signal_desk_gold_authoring_budget_grant.json",
        session_root=Path.home() / ".codex/sessions", budget_dir=ROOT / "work/pif-ops/budget",
        split="development", turn_type="AUDIT", task_namespace="corrected-dev-audit-v1",
        concurrency=a.concurrency))
    (OUT / "run.json").write_text(json.dumps(run, indent=2, sort_keys=True) + "\n")
    if not run.get("complete"):
        return 2
    audit = evaluate_dev_audit(manifest_path=MANIFEST, result_root=OUT / "results",
        initial_window_ids=plan["initial_ids"], project_root=ROOT)
    (OUT / "audit.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"audit_passed": audit["passed"], "accepted_gold": False,
                      "remaining": "fresh semantic adjudication, atomicity and residual-speaker review"}))
    return 0 if audit["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
