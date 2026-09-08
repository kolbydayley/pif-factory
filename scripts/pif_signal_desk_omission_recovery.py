#!/usr/bin/env python3
"""Metered targeted recovery after the complete diagnostic run has settled."""
import argparse
import asyncio
import fcntl
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_omission_review import OUT as ORIGINAL, QUAL, prepare as original_prepare
from scripts.pif_signal_desk_attribution_probe_review import execute as metered_review
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_omission_review import validate, schema
from research_factory.signal_desk_omission_recovery import SYSTEM, repair_packets, reconcile
from research_factory.signal_desk_rubric_reference_packets import digest
OUT = ORIGINAL / "recovery-v1"


def prepare():
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base")
    originals = original_prepare()  # Rechecks source/candidate/full role lineage.
    inventory = {}; selected = []
    # Refuse partial acquisition: the complete original population must settle.
    for p in originals:
        sha = p["packet_sha256"]
        valid = ORIGINAL / f"{sha}.review.json"
        pending = ORIGINAL / f"{sha}.pending.json"
        if valid.exists():
            validate(json.loads(valid.read_text()), p)
            if pending.exists(): raise ValueError("original packet has conflicting valid/pending outputs")
            continue
        if not pending.exists(): raise ValueError("original review has not fully settled")
        sidecar = json.loads((ORIGINAL / f"{sha}.sidecar.json").read_text())
        if sidecar.get("state") != "completed" or sidecar.get("error_class"):
            raise ValueError("provider attempt requires separate transport recovery")
        raw = json.loads((ORIGINAL / f"{sha}.output.json").read_text())
        try: validate(raw, p)
        except ValueError: pass
        else: raise ValueError("pending packet unexpectedly valid; inspect before recovery")
        parts, ps = repair_packets(p, raw, token_count=lambda s: len(enc.encode(s)))
        inventory[sha] = {"partition": parts, "packets": [q["packet_sha256"] for q in ps],
            "sidecar_sha256": digest(sidecar), "pending_sha256": digest(json.loads(pending.read_text()))}
        selected.extend(ps)
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for p in selected: immutable_json(OUT / f"{p['packet_sha256']}.packet.json", p)
    plan = {"original_plan_sha256": digest(json.loads((ORIGINAL / "plan.json").read_text())),
        "inventory": inventory, "packets": [p["packet_sha256"] for p in selected],
        "system": SYSTEM, "system_sha256": digest(SYSTEM), "schema_sha256": digest(schema()),
        "model": "gpt-5.5", "effort": "high", "concurrency": 1,
        "first_pass_failure_preserved": True, "gold_accepted": False, "gate_eligible": False}
    immutable_json(OUT / "plan.json", plan)
    return originals, selected, plan


def collect(originals, selected, plan, *, write=False):
    """Recompute reconciliations; never trust a saved assembled output alone."""
    by_sha = {p["packet_sha256"]: p for p in originals}
    repairs = {p["packet_sha256"]: p for p in selected}
    outputs = {}; receipts = {}; unresolved = []
    for sha, row in plan["inventory"].items():
        values = []
        for rsha in row["packets"]:
            path = OUT / f"{rsha}.review.json"
            if not path.exists(): break
            values.append((repairs[rsha], json.loads(path.read_text())))
        if len(values) != len(row["packets"]): unresolved.append(sha); continue
        raw = json.loads((ORIGINAL / f"{sha}.output.json").read_text())
        output, receipt = reconcile(by_sha[sha], raw, values)
        if receipt["partition"] != row["partition"]: raise ValueError("recovery inventory changed")
        if write:
            immutable_json(OUT / "reconciled" / f"{sha}.review.json", output)
            immutable_json(OUT / "reconciled" / f"{sha}.receipt.json", receipt)
        outputs[sha] = output; receipts[sha] = receipt
    return outputs, receipts, unresolved


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    # Same original lock, plus recovery lock: no parallel original/recovery lane.
    with (QUAL / "source-disagreement-review.lock").open("a") as original_lock, (QUAL / "source-disagreement-recovery.lock").open("a") as lock:
        fcntl.flock(original_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        originals, selected, plan = prepare()
        print(json.dumps({"failed_original_packets": len(plan["inventory"]), "repair_packets": len(selected),
            "repair_cases": sum(len(p["cases"]) for p in selected), "gold_accepted": False}), flush=True)
        if args.execute and selected:
            asyncio.run(metered_review(selected, output_root=OUT, task_prefix="full-event-v3-source-disagreement-recovery-v1",
                system=SYSTEM, schema_for_packet=lambda p: schema(), validator=validate,
                packet_id=lambda p: p["packet_sha256"], verdict_rows=lambda v: v["decisions"]))
        if args.execute:
            outputs, receipts, unresolved = collect(originals, selected, plan, write=True)
            result = {"reconciled_original_packets": len(outputs), "unresolved_original_packets": unresolved,
                "receipt_inventory": {sha: digest(r) for sha, r in receipts.items()},
                "first_pass_failure_preserved": True, "gold_accepted": False, "gate_eligible": False}
            immutable_json(OUT / "reconciliation-receipts" / f"{digest(result)}.json", result)
            print(json.dumps(result), flush=True)
            raise SystemExit(2 if unresolved else 0)


if __name__ == "__main__": main()
