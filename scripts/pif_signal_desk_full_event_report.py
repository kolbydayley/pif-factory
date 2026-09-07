#!/usr/bin/env python3
"""Full qualification diagnostic; preserves denominators and pending semantics."""
from collections import Counter
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_full_event_review import prepare, OUT, QUAL, BASE
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_review import validate_review
from research_factory.signal_desk_full_event_comparison import compare
from research_factory.signal_desk_rubric_reference_packets import digest


def summarize(packets, reviews, authored, structures):
    expected = {p["packet_sha256"] for p in packets}
    if set(reviews) != expected or len(expected) != len(packets): raise ValueError("incomplete review packet set")
    windows = {p["window_id"] for p in packets}
    if windows != set(authored) or windows != set(structures): raise ValueError("incomplete window population")
    verdicts = Counter(); unresolved = []; seen = set(); inventory = {}; comparisons = []; sources = {}
    for p in packets:
        wid = p["window_id"]; sha = p["packet_sha256"]
        if digest({k: v for k, v in p.items() if k != "packet_sha256"}) != sha: raise ValueError("review packet digest changed")
        if p["original_output_sha256"] != digest(authored[wid]["C"]): raise ValueError("changed C candidate population")
        originals = {e["event_id"]: e for e in authored[wid]["C"]["events"]}
        if any(originals.get(e["event_id"]) != e for e in p["candidates"]): raise ValueError("changed review candidate")
        if wid in sources and sources[wid] != p["transcript_window"]: raise ValueError("inconsistent source context")
        sources[wid] = p["transcript_window"]
        value = validate_review(reviews[sha], source=sources[wid], window_id=wid, candidates=p["candidates"])
        inventory[sha] = digest(value)
        for row in value["decisions"]:
            key = (wid, row["event"]["event_id"])
            if key in seen: raise ValueError("candidate reviewed twice")
            seen.add(key); verdicts[row["verdict"]] += 1
            if row["verdict"] != "supported":
                unresolved.append({"window_id": wid, "event_id": key[1], "verdict": row["verdict"], "review_packet_sha256": sha})
        if not p["candidates"]:
            verdicts[value["empty_window_verdict"]] += 1
            if value["empty_window_verdict"] != "supported_empty":
                unresolved.append({"window_id": wid, "event_id": None, "verdict": value["empty_window_verdict"], "review_packet_sha256": sha})
    expected_events = {(w, e["event_id"]) for w in windows for e in authored[w]["C"]["events"]}
    if seen != expected_events: raise ValueError("candidate denominator loss")
    by_role = {}; by_structure = {}
    for wid in sorted(windows):
        if set(authored[wid]) != {"A", "B", "C", "AUDIT"}: raise ValueError("missing qualification role")
        for role in ("A", "B", "C", "AUDIT"):
            value = authored[wid][role]
            from research_factory.signal_desk_full_event_experiment import validate
            validate(value, source=sources[wid], window_id=wid)
            row = by_role.setdefault(role, Counter()); sr = by_structure.setdefault(structures[wid], {}).setdefault(role, Counter())
            for count in (row, sr):
                count["windows"] += 1; count["events"] += len(value["events"])
                count["empty_windows"] += int(not value["events"])
                count["unknown_voice"] += sum(e["attribution"]["transcript_voice"] is None for e in value["events"])
                count["unknown_owner"] += sum(e["attribution"]["proposition_owner"] is None for e in value["events"])
            if role != "C": comparisons.append(compare(authored[wid]["C"], value, source=sources[wid], structure=structures[wid], role=role))
    return {"windows": len(windows), "candidate_events": len(expected_events), "verdicts": dict(verdicts),
        "by_role": by_role, "by_structure": by_structure, "comparisons": comparisons,
        "unresolved_final_review_proposals": unresolved, "review_inventory": inventory,
        "qualified": False, "gold_accepted": False, "gate_eligible": False,
        "next": "Source-adjudicate disagreements and final proposals; qualification does not replace full reliability audits or A1/A2."}


def main():
    packets = prepare()  # Revalidates all original sources, role packets and outputs.
    authored = {}; structures = {}
    for wid in {p["window_id"] for p in packets}:
        authored[wid] = {}
        for role in ("A", "B", "C", "AUDIT"):
            path = QUAL / "calls" / wid / role
            p = json.loads((path / "packet.json").read_text()); structures[wid] = p["transcript_structure"]
            authored[wid][role] = json.loads((path / f"{p['packet_sha256']}.result.json").read_text())
    reviews = {p["packet_sha256"]: json.loads((OUT / f"{p['packet_sha256']}.review.json").read_text()) for p in packets}
    result = summarize(packets, reviews, authored, structures)
    plan = json.loads((QUAL / "plan.json").read_text())
    prior = {}
    for wid, sha in plan["source_packets"].items():
        original = json.loads((BASE / f"{sha}.packet.json").read_text())
        if digest({k: v for k, v in original.items() if k != "packet_sha256"}) != sha:
            raise ValueError("original candidate inventory binding changed")
        prior[wid] = {"prior_candidate_events": original["candidate_event_count"],
            "current_C_events": len(authored[wid]["C"]["events"]), "source_packet_sha256": sha,
            "source_review_required": True}
    result["prior_candidate_count_comparison"] = prior
    result["prior_candidate_events"] = sum(v["prior_candidate_events"] for v in prior.values())
    result["candidate_count_warning"] = "Count reduction is not a quality gain; prior/new disagreement needs source review. Prior candidates are not assumed correct."
    immutable_json(QUAL / "diagnostic-report.json", result)
    print(json.dumps({"windows": result["windows"], "candidate_events": result["candidate_events"], "verdicts": result["verdicts"], "qualified": False}))


if __name__ == "__main__": main()
