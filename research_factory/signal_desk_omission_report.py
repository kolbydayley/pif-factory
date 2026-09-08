"""Denominator-preserving reconciliation of source disagreement diagnostics."""
from collections import Counter
from .signal_desk_omission_review import validate, SYSTEM, schema
from .signal_desk_rubric_reference_packets import digest


def summarize(plan, packets, reviews, *, allow_partial=False):
    expected = plan["packets"]
    if len(set(expected)) != len(expected) or set(packets) != set(expected):
        raise ValueError("changed packet population")
    if not set(reviews) <= set(expected): raise ValueError("foreign review")
    pending = sorted(set(expected) - set(reviews))
    if pending and not allow_partial: raise ValueError("review is incomplete")
    by_origin = {}; by_structure = {}; actions = []; seen = {}; inventory = {}
    for sha in expected:
        packet = packets[sha]
        if packet.get("packet_sha256") != sha or digest({k: v for k, v in packet.items() if k != "packet_sha256"}) != sha:
            raise ValueError("changed packet bytes")
        if packet["system_sha256"] != digest(SYSTEM) or packet["schema_sha256"] != digest(schema()):
            raise ValueError("changed diagnostic contract")
        if packet["source_sha256"] != digest(packet["transcript_window"]): raise ValueError("changed source")
        wid = packet["window_id"]
        if wid not in plan["ledger"]: raise ValueError("unknown window")
        ids = seen.setdefault(wid, set())
        for case in packet["cases"]:
            if case["case_id"] in ids: raise ValueError("duplicate case across packets")
            ids.add(case["case_id"])
        value = validate(reviews[sha], packet) if sha in reviews else None
        decisions = {d["case_id"]: d for d in value["decisions"]} if value else {}
        if value: inventory[sha] = digest(value)
        for case in packet["cases"]:
            decision = decisions.get(case["case_id"])
            for bucket, key in ((by_origin, case["origin"]), (by_structure, packet["transcript_structure"])):
                counts = bucket.setdefault(key, Counter()); counts["expected_cases"] += 1
                counts["reviewed_cases"] += int(decision is not None)
                if decision:
                    for field in ("verdict", "utility", "coverage", "correction"):
                        counts[field + ":" + decision[field]] += 1
            if not decision: continue
            reasons = []
            if case["kind"] == "candidate" and case["origin"] != "current_C" and decision["verdict"] == "supported" and decision["utility"] == "consequential" and decision["coverage"] in {"missing", "partial"}:
                reasons.append("potential_consequential_omission")
            if case["origin"] == "current_C" and decision["utility"] in {"promotion_or_chrome", "incidental"}:
                reasons.append("current_candidate_usefulness_exclusion")
            if decision["verdict"] != "supported": reasons.append("source_fidelity_resolution")
            if decision["utility"] == "borderline": reasons.append("usefulness_boundary_resolution")
            if case["kind"] == "correction": reasons.append("final_correction_reconciliation")
            if reasons:
                # Unique exact strings are validated before deterministic location.
                locations = [{"start": packet["transcript_window"].index(q),
                    "end": packet["transcript_window"].index(q) + len(q)} for q in decision["source_quotes"]]
                actions.append({"window_id": wid, "case_id": case["case_id"], "origin": case["origin"],
                    "packet_sha256": sha, "review_sha256": inventory[sha], "reasons": reasons,
                    "diagnosis": {k: decision[k] for k in ("verdict", "utility", "coverage", "correction", "current_C_ids")},
                    "source_locations": locations, "gold_accepted": False})
    if set(seen) != set(plan["ledger"]): raise ValueError("missing window")
    for wid, row in plan["ledger"].items():
        if len(row["case_ids"]) != row["cases"] or len(set(row["case_ids"])) != row["cases"] or seen[wid] != set(row["case_ids"]):
            raise ValueError("case ledger mismatch")
    return {"windows": len(seen), "expected_packets": len(expected), "reviewed_packets": len(reviews),
        "expected_cases": sum(map(len, seen.values())), "pending_packets": pending,
        "by_origin": by_origin, "by_structure": by_structure, "actions": actions,
        "review_inventory": inventory, "plan_sha256": digest(plan),
        "complete_diagnostic": not pending, "gold_accepted": False, "gate_eligible": False,
        "denominator_warning": "Cases include overlapping versions of claims. These are diagnostic case counts, not independent events or corpus error-rate estimates.",
        "next": "Source-reconcile actionable cases before any repair; diagnose omission/contamination versus harmless paraphrase. This report never applies labels or clears qualification."}
