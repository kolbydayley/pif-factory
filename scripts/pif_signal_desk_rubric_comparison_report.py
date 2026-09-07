#!/usr/bin/env python3
"""Complete, source-bound diagnostic comparison; never an acceptance gate."""
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_rubric_reference_review import prepare, QUAL, OUT, R
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_gold_audit import _load_frozen_window_text
from research_factory.signal_desk_rebuild_contracts import validate_output
from research_factory.signal_desk_rubric_reference_packets import validate_reference, digest
from research_factory.signal_desk_rubric_comparison import compare_role
from research_factory.signal_desk_reference_recovery import load_reference
from research_factory.signal_desk_rubric_reference_packets import REFERENCE_SYSTEM
from scripts.pif_signal_desk_rubric_reference_format_retry import FORMAT_CONTRACT


def main():
    packets = prepare()  # Revalidates all frozen source-bound A/B/C and plan hashes.
    plan = json.loads((QUAL / "plan.json").read_text())
    manifest = json.loads((R / "merged-manifest.json").read_text())
    windows = {w["window_id"]: w for w in manifest["windows"] if w["split"] == "development"}
    reviews = []; inventory = {}; verdicts = Counter(); relevance = Counter(); recoveries = []
    # Require every review; a partial sample must not look like full qualification.
    for packet in packets:
        reference, bindings, recovery = load_reference(OUT, packet,
            expected_retry_system_sha256=digest(REFERENCE_SYSTEM + FORMAT_CONTRACT))
        inventory.update(bindings)
        if recovery["retried"]: recoveries.append(recovery)
        for decision in reference["decisions"]:
            verdicts[decision["verdict"]] += 1
            relevance[decision["strategic_relevance"]] += 1
            if decision["verdict"] != "supported":
                reviews.append({"window_id": packet["window_id"],
                    "event_id": decision["event_id"], "packet_sha256": packet["packet_sha256"],
                    "verdict": decision["verdict"], "resolved": False})
        if reference["empty_window_verdict"] in {"missed_events", "unresolved"}:
            reviews.append({"window_id": packet["window_id"], "event_id": None,
                "packet_sha256": packet["packet_sha256"],
                "verdict": reference["empty_window_verdict"], "resolved": False})
    comparisons = []; strata = {}; speakers = []
    for wid in plan["window_ids"]:
        row = windows[wid]
        text = _load_frozen_window_text(row, project_root=ROOT)
        outputs = {}
        for role in ("A", "B", "C", "AUDIT"):
            output = json.loads((QUAL / "results" / role / f"{wid}.json").read_text())
            validate_output(output, transcript_window=text, expected_window_id=wid)
            inventory[f"{wid}:{role}"] = digest(output)
            outputs[role] = output
            speakers.append({"window_id": wid, "role": role,
                "transcript_structure": row["transcript_structure"], "alignment": row.get("alignment"),
                "events": len(output["events"]),
                "unassigned_speaker": sum(not e.get("speaker_id") for e in output["events"])})
        for role in ("A", "B", "AUDIT"):
            result = compare_role(outputs["C"], outputs[role], role=role,
                transcript_structure=row["transcript_structure"])
            comparisons.append(result)
            key = role + ":" + row["transcript_structure"]
            counts = strata.setdefault(key, Counter())
            counts["windows"] += 1
            counts["matched_events"] += result["matched_events"]
            for field, n in result["field_denominators"].items():
                counts[field + "_denominator"] += n
                counts[field + "_agreement"] += result["field_agreement_counts"][field]
    report = {"source_inventory": inventory, "window_count": len(plan["window_ids"]),
        "reference_recoveries": recoveries, "first_pass_failed_packets": len(recoveries),
        "reference_verdicts": dict(verdicts), "strategic_relevance": dict(relevance),
        "unresolved_reference_decisions": reviews, "comparisons": comparisons,
        "per_role_stratum": {k: dict(v) for k, v in strata.items()}, "speaker_coverage": speakers,
        "comparison_anchor": "candidate C, not accepted truth",
        "gold_accepted": False, "rubric_qualified": False, "gate_eligible": False,
        "next": "Source-adjudicate corrections and role disagreements; null speaker counts are not fabrication rates."}
    immutable_json(QUAL / "role-comparison.json", report)
    print(json.dumps({"windows": report["window_count"], "reference_verdicts": dict(verdicts),
        "unresolved_reference_decisions": len(reviews), "gold_accepted": False}))


if __name__ == "__main__":
    main()
