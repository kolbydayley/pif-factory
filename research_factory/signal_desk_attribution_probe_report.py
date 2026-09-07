"""Diagnostic disagreements with complete denominators, never a gold gate."""
from collections import Counter
from .signal_desk_attribution_probe_review import build_packet, validate_review
from .signal_desk_rubric_reference_packets import digest


def summarize(packets, reviews):
    totals = Counter(); strata = {}; changes = []; inventory = {}
    expected = {p["review_packet_sha256"] for p in packets}
    if len(expected) != len(packets) or set(reviews) != expected:
        raise ValueError("missing, duplicate, or unexpected review packets")
    seen_windows = set()
    for packet in packets:
        original = packet["original_packet"]; proposed = packet["proposed_labels"]
        if packet != build_packet(original, proposed):
            raise ValueError("review packet provenance changed")
        wid = original["window_id"]
        if wid in seen_windows: raise ValueError("duplicate window")
        seen_windows.add(wid)
        sha = packet["review_packet_sha256"]
        review = validate_review(reviews[sha], original, proposed)
        inventory[wid] = {"packet_sha256": sha, "review_sha256": digest(review)}
        row = strata.setdefault(original["transcript_structure"], Counter())
        for counts in (totals, row):
            counts["windows"] += 1
            counts["candidate_events"] += original["candidate_event_count"]
            counts["anchors"] += len(original["anchors"])
            counts["empty_anchor_windows"] += int(not original["anchors"])
        initial = {d["event_id"]: d for d in proposed["decisions"]}
        for judged in review["reviews"]:
            decision = judged["decision"]; before = initial[decision["event_id"]]
            for counts in (totals, row):
                counts[judged["verdict"]] += 1
                counts["proposed_unknown_voice"] += int(before["attribution"]["transcript_voice"] is None)
                counts["review_unknown_voice"] += int(decision["attribution"]["transcript_voice"] is None)
                counts["proposed_unknown_owner"] += int(before["attribution"]["proposition_owner"] is None)
                counts["review_unknown_owner"] += int(decision["attribution"]["proposition_owner"] is None)
            if judged["verdict"] != "supported":
                changed = [k for k in ("claim_status", "stance") if before[k] != decision[k]]
                changed += ["attribution." + k for k in before["attribution"] if before["attribution"][k] != decision["attribution"][k]]
                changes.append({"window_id": wid, "event_id": decision["event_id"],
                    "verdict": judged["verdict"], "changed_fields": changed,
                    "source_adjudication_required": True})
    return {"totals": dict(totals), "by_structure": {k: dict(v) for k, v in strata.items()},
        "unresolved_review_proposals": changes, "inventory": inventory,
        "gold_accepted": False, "qualified": False, "gate_eligible": False,
        "representative_corpus_estimate": False,
        "limitations": ["Fixed candidate anchors do not measure omissions or recall.",
            "Reviewer agreement is not proof of correctness; corrections need source inspection.",
            "Unknown identities are reported, never fabricated or dropped from denominators."]}
