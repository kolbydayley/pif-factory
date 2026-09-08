#!/usr/bin/env python3
"""Lossless failed-row recovery, only after all eight reviews have settled."""
import argparse
import asyncio
from copy import deepcopy
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_semantic_contract_review import OUT as ORIGINAL, SOURCE, prepare as original_prepare, validate, schema, SYSTEM as FIRST_SYSTEM
from scripts.pif_signal_desk_attribution_probe_review import execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_rubric_reference_packets import digest

OUT = ORIGINAL / "recovery-v1"
SYSTEM = FIRST_SYSTEM + """
RECOVERY: prior_invalid_decisions are failed outputs, not an answer key.
Return only the supplied case IDs. source_quotes must quote transcript_window,
NOT proposed_rules, JSON Schema, candidate paraphrases, or the packet wrapper.
Do not add enclosing quote characters absent from the transcript itself.
For __contract__ explain schema reasoning in rationale/proposed_rule_change,
but use source_quotes for the real transcript distinction the rule must handle.
Inspect the actual nested properties/required structure before alleging a schema
defect. Do not repeat prior claims without checking them. Retain uncertainty.
"""


def partition(packet, raw):
    if not isinstance(raw, dict) or set(raw) != {"decisions"} or not isinstance(raw["decisions"], list) or any(not isinstance(r, dict) for r in raw["decisions"]):
        raise ValueError("malformed output needs separate investigation")
    expected = {c["case_id"]: c for c in packet["cases"]}
    if len(expected) != len(packet["cases"]): raise ValueError("duplicate input")
    retained = []; failures = []
    for cid, case in expected.items():
        rows = [r for r in raw["decisions"] if r.get("case_id") == cid]
        try: validate({"decisions": rows}, dict(packet, cases=[case]))
        except ValueError as exc: failures.append({"case_id": cid, "reason": str(exc), "prior_invalid_decisions": deepcopy(rows)})
        else: retained.append(deepcopy(rows[0]))
    return {"original_packet_sha256": digest(packet), "original_output_sha256": digest(raw),
        "retained": retained, "failures": failures,
        "extras": [deepcopy(r) for r in raw["decisions"] if r.get("case_id") not in expected],
        "gold_accepted": False}


def repair_packets(packet, raw, token_count):
    parts = partition(packet, raw); result = []
    originals = {c["case_id"]: c for c in packet["cases"]}
    # One failed case per packet preserves full source and limits long prior answers.
    for failure in parts["failures"]:
        p = deepcopy(packet)
        p.pop("packet_sha256")
        p.update(parent_packet_sha256=packet["packet_sha256"], original_output_sha256=digest(raw),
            partition_sha256=digest(parts), system_sha256=digest(SYSTEM),
            cases=[deepcopy(originals[failure["case_id"]])], failure=deepcopy(failure))
        p["packet_sha256"] = digest(p)
        if token_count(SYSTEM + json.dumps(p, ensure_ascii=False) + json.dumps(schema())) + 1500 > 12000:
            raise ValueError("full-source repair exceeds token limit; do not truncate")
        result.append(p)
    return parts, result


def reconcile(original, raw, repairs):
    parts = partition(original, raw)
    required = {r["case_id"] for r in parts["failures"]}; fixed = {}; lineage = {}
    for p, value in repairs:
        cid = p["cases"][0]["case_id"]
        if cid not in required or cid in fixed or len(p["cases"]) != 1: raise ValueError("wrong repair population")
        expected = deepcopy(original); expected.pop("packet_sha256")
        expected.update(parent_packet_sha256=original["packet_sha256"], original_output_sha256=digest(raw),
            partition_sha256=digest(parts), system_sha256=digest(SYSTEM),
            cases=[next(c for c in original["cases"] if c["case_id"] == cid)],
            failure=next(f for f in parts["failures"] if f["case_id"] == cid))
        expected["packet_sha256"] = digest(expected)
        if p != expected: raise ValueError("repair provenance changed")
        validate(value, p); fixed[cid] = value["decisions"][0]; lineage[p["packet_sha256"]] = digest(value)
    if set(fixed) != required: raise ValueError("missing repair")
    rows = {r["case_id"]: r for r in parts["retained"]}; rows.update(fixed)
    value = {"decisions": [rows[c["case_id"]] for c in original["cases"]]}
    validate(value, original)
    return value, {"partition": parts, "repair_inventory": lineage, "reconciled_sha256": digest(value),
        "first_pass_failure_preserved": True, "gold_accepted": False, "gate_eligible": False}


def prepare():
    import tiktoken
    originals = original_prepare(); enc = tiktoken.get_encoding("o200k_base")
    inventory = {}; selected = []
    for p in originals:
        sha = p["packet_sha256"]; target = ORIGINAL / f"{sha}.review.json"
        pending = ORIGINAL / f"{sha}.pending.json"
        if target.exists():
            validate(json.loads(target.read_text()), p)
            if pending.exists(): raise ValueError("conflicting state")
            continue
        if not pending.exists(): raise ValueError("original campaign has not settled")
        sidecar = json.loads((ORIGINAL / f"{sha}.sidecar.json").read_text())
        if sidecar.get("state") != "completed" or sidecar.get("error_class"): raise ValueError("transport failure needs separate recovery")
        raw = json.loads((ORIGINAL / f"{sha}.output.json").read_text())
        parts, packets = repair_packets(p, raw, lambda text: len(enc.encode(text)))
        if not parts["failures"]: raise ValueError("held but no invalid rows; inspect rather than redispatch")
        inventory[sha] = {"partition": parts, "packets": [r["packet_sha256"] for r in packets]}
        selected.extend(packets)
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for p in selected: immutable_json(OUT / f"{p['packet_sha256']}.packet.json", p)
    immutable_json(OUT / "plan.json", {"inventory": inventory, "packets": [p["packet_sha256"] for p in selected],
        "system": SYSTEM, "schema_sha256": digest(schema()), "original_plan_sha256": digest(json.loads((ORIGINAL / "plan.json").read_text())),
        "gold_accepted": False, "gate_eligible": False})
    return originals, selected, inventory


def validate_packet_inventory(expected_packets, dispatch_packets):
    # Immutable JSON sorts mapping keys; the dispatch list preserves preparation
    # order. Mapping iteration order is not provenance.
    if set(expected_packets) != set(dispatch_packets) or len(set(expected_packets)) != len(expected_packets) or len(set(dispatch_packets)) != len(dispatch_packets):
        raise ValueError("changed recovery packet inventory")


def collect(original_root=ORIGINAL, recovery_root=OUT):
    """Read and recompute recovery; saved reconciliations are not authority."""
    plan = json.loads((recovery_root / "plan.json").read_text())
    original_plan = json.loads((original_root / "plan.json").read_text())
    if plan["original_plan_sha256"] != digest(original_plan) or plan["system"] != SYSTEM or plan["schema_sha256"] != digest(schema()):
        raise ValueError("changed recovery plan lineage")
    if set(plan["inventory"]) - set(original_plan["packets"]):
        raise ValueError("unknown original in recovery inventory")
    expected_packets = [r for entry in plan["inventory"].values() for r in entry["packets"]]
    validate_packet_inventory(expected_packets, plan["packets"])
    outputs = {}; receipts = {}; pending = []
    for sha, entry in plan["inventory"].items():
        original = json.loads((original_root / f"{sha}.packet.json").read_text())
        raw = json.loads((original_root / f"{sha}.output.json").read_text())
        if digest({k: v for k, v in original.items() if k != "packet_sha256"}) != sha or original["packet_sha256"] != sha:
            raise ValueError("changed original packet")
        if partition(original, raw) != entry["partition"]:
            raise ValueError("changed original failed output")
        pairs = []; sidecars = {}
        for rsha in entry["packets"]:
            target = recovery_root / f"{rsha}.review.json"
            if not target.exists(): break
            if (recovery_root / f"{rsha}.pending.json").exists(): raise ValueError("conflicting recovery state")
            packet = json.loads((recovery_root / f"{rsha}.packet.json").read_text())
            value = json.loads(target.read_text())
            sidecar = json.loads((recovery_root / f"{rsha}.sidecar.json").read_text())
            if sidecar.get("state") != "completed" or sidecar.get("error_class") or json.loads((recovery_root / f"{rsha}.output.json").read_text()) != value:
                raise ValueError("recovery not backed by completed raw provider output")
            if packet["packet_sha256"] != rsha: raise ValueError("wrong repair filename")
            pairs.append((packet, value)); sidecars[rsha] = digest(sidecar)
        if len(pairs) != len(entry["packets"]): pending.append(sha); continue
        output, receipt = reconcile(original, raw, pairs)
        receipt["sidecar_inventory"] = sidecars
        outputs[sha] = output; receipts[sha] = receipt
    return outputs, receipts, pending


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    with (SOURCE / "semantic-contract-review.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        originals, packets, inventory = prepare()
        print(json.dumps({"held_packets": len(inventory), "repair_calls": len(packets),
            "retained_valid_rows": sum(len(r["partition"]["retained"]) for r in inventory.values()), "gold_accepted": False}), flush=True)
        if not args.execute: return
        code = asyncio.run(execute(packets, output_root=OUT, task_prefix="semantic-contract-recovery-v1", system=SYSTEM,
            schema_for_packet=lambda p: schema(), validator=validate, packet_id=lambda p: p["packet_sha256"], verdict_rows=lambda v: v["decisions"]))
        by_id = {p["packet_sha256"]: p for p in packets}
        for p in originals:
            sha = p["packet_sha256"]
            if sha not in inventory: continue
            ids = inventory[sha]["packets"]
            if not all((OUT / f"{r}.review.json").exists() for r in ids): continue
            raw = json.loads((ORIGINAL / f"{sha}.output.json").read_text())
            value, receipt = reconcile(p, raw, [(by_id[r], json.loads((OUT / f"{r}.review.json").read_text())) for r in ids])
            immutable_json(OUT / "reconciled" / f"{sha}.review.json", value)
            immutable_json(OUT / "reconciled" / f"{sha}.receipt.json", receipt)
        raise SystemExit(code)


if __name__ == "__main__": main()
