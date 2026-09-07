"""Diagnostic role comparisons: matching candidates are not adjudicated errors."""
from collections import Counter
from .signal_desk_rebuild_scorer import match_events, SCORER_VERSION, scorer_sha256


def compare_role(reference, prediction, *, role, transcript_structure):
    if reference["window_id"] != prediction["window_id"]:
        raise ValueError("cross-window comparison prohibited")
    matched = match_events(reference["events"], prediction["events"],
                           transcript_structure=transcript_structure)
    fields = Counter(); totals = Counter(); candidates = []
    gold_indexes = set(); predicted_indexes = set()
    for pair in matched["matches"]:
        gi, pi = pair["gold_index"], pair["predicted_index"]
        gold_indexes.add(gi); predicted_indexes.add(pi)
        disagreements = []
        for field, agrees in pair["field_agreement"].items():
            totals[field] += 1; fields[field] += int(agrees)
            if not agrees: disagreements.append(field)
        if disagreements:
            candidates.append({"reference_event_id":reference["events"][gi]["event_id"],
                "predicted_event_id":prediction["events"][pi]["event_id"],
                "disagreement_fields":disagreements, "confirmed_error":False})
    return {"window_id":reference["window_id"], "role":role,
        "transcript_structure":transcript_structure, "scorer_version":SCORER_VERSION,
        "scorer_sha256":scorer_sha256(), "matched_events":len(gold_indexes),
        "reference_events":len(reference["events"]), "predicted_events":len(prediction["events"]),
        "both_empty":not reference["events"] and not prediction["events"],
        "field_agreement_counts":dict(fields), "field_denominators":dict(totals),
        "unmatched_reference_ids":[e["event_id"] for i,e in enumerate(reference["events"]) if i not in gold_indexes],
        "unmatched_prediction_ids":[e["event_id"] for i,e in enumerate(prediction["events"]) if i not in predicted_indexes],
        "review_candidates":candidates, "source_adjudication_required":True,
        "gold_accepted":False, "rubric_qualified":False, "gate_eligible":False}
