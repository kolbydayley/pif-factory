"""Provenance-checked diagnostic review summary; never a qualification gate."""
from collections import Counter
import json
from .signal_desk_rubric_reference_packets import digest


def summarize(root, validator):
    plan = json.loads((root / "plan.json").read_text())
    ids = plan["packets"]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("invalid packet inventory")
    families = {f["family_id"]: f for f in plan["families"]}
    if len(families) != len(plan["families"]):
        raise ValueError("duplicate family")
    rows = []; decisions = Counter(); actions = []; lineage = {}
    for sha in ids:
        packet = json.loads((root / f"{sha}.packet.json").read_text())
        if packet.get("packet_sha256") != sha or digest({k: v for k, v in packet.items() if k != "packet_sha256"}) != sha:
            raise ValueError("changed packet")
        if digest(packet["transcript_window"]) != packet["source_sha256"]:
            raise ValueError("changed source")
        if packet["system_sha256"] != digest(plan["system"]) or packet["review_schema_sha256"] != plan["review_schema_sha256"]:
            raise ValueError("changed review contract")
        family = packet["family"]["family_id"]
        if families.get(family) != packet["family"]:
            raise ValueError("changed experiment family")
        cases = packet["cases"]
        if len({c["case_id"] for c in cases}) != len(cases) or not 1 <= len(cases) <= 25:
            raise ValueError("invalid case population")
        target = root / f"{sha}.review.json"
        pending = root / f"{sha}.pending.json"
        row = {"packet_sha256": sha, "family_id": family,
            "window_id": packet["window_id"], "expected_cases": len(cases)}
        if target.exists():
            if pending.exists(): raise ValueError("conflicting valid/pending state")
            value = validator(json.loads(target.read_text()), packet)
            sidecar = json.loads((root / f"{sha}.sidecar.json").read_text())
            if sidecar.get("state") != "completed" or sidecar.get("error_class"):
                raise ValueError("review without completed provider receipt")
            raw = json.loads((root / f"{sha}.output.json").read_text())
            if raw != value:
                raise ValueError("review differs from saved raw output")
            lineage[sha] = {"review_sha256": digest(value), "sidecar_sha256": digest(sidecar)}
            counts = Counter(d["verdict"] for d in value["decisions"])
            decisions.update(counts)
            row.update(state="validated_diagnostic", verdicts=dict(counts))
            for decision in value["decisions"]:
                if decision["verdict"] != "supported" or decision["proposed_rule_change"].strip():
                    actions.append({"packet_sha256": sha, "family_id": family,
                        "window_id": packet["window_id"], "case_id": decision["case_id"],
                        "verdict": decision["verdict"], "has_rule_proposal": bool(decision["proposed_rule_change"].strip())})
        else:
            row["state"] = "held_contract_failure" if pending.exists() else "not_yet_validated"
        rows.append(row)
    return {"plan_sha256": digest(plan), "packets": rows, "review_lineage": lineage,
        "expected_cases": sum(r["expected_cases"] for r in rows),
        "validated_cases": sum(decisions.values()), "verdicts": dict(decisions),
        "action_references": actions,
        "complete": all(r["state"] == "validated_diagnostic" for r in rows),
        "qualified": False, "gold_accepted": False, "gate_eligible": False,
        "sampling": plan["sampling"]}
