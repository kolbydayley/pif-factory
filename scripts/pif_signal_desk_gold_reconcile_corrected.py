#!/usr/bin/env python3
"""Reconcile corrected dev review lineage without mutating Gold-C or its denominator."""
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research_factory.signal_desk_gold_disagreement import build_cases
from research_factory.signal_desk_gold_audit import evaluate_adjudicated_gold_reliability
from scripts.pif_signal_desk_gold_review_corrected import R, A, packet_cases, validate_packet_decisions, _sha_json
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json


def reconcile():
    audit = json.loads((A / "audit.json").read_text())
    cases = build_cases(manifest_path=R / "merged-manifest.json", result_root=A / "results", project_root=ROOT,
                        selected_window_ids=audit["audit_selection"]["window_ids"])
    expected = {c["case_id"]: c for c in cases}
    decisions, lineage = {}, {}
    for name in ("corrected-review-full-batched-v2", "corrected-review-malformed-retry-v1", "corrected-review-oversized-v1"):
        directory = R / name
        plan = json.loads((directory / "plan.json").read_text())
        for digest in plan["packet_digests"]:
            path = directory / f"{digest}.decision.json"
            if not path.exists():
                continue
            packet = json.loads((directory / f"{digest}.packet.json").read_text())
            if _sha_json({k: v for k, v in packet.items() if k != "packet_sha256"}) != digest:
                raise ValueError("packet hash changed")
            output = json.loads(path.read_text())
            validated = validate_packet_decisions(output, packet)
            for case in packet_cases(packet):
                cid = case["case_id"]
                if cid in decisions or cid not in expected:
                    raise ValueError("duplicate or unexpected case")
                original = expected[cid]
                if case["gold"] != {k: v for k, v in original["gold"].items() if k not in {"speaker_role", "quoted_person", "mentioned_people"}}:
                    raise ValueError("reviewed Gold-C differs from frozen audit")
                decisions[cid] = validated[cid]
                lineage[cid] = {"directory": name, "packet_sha256": digest, "output_sha256": _sha_json(output)}
    if set(decisions) != set(expected):
        raise ValueError("review inventory incomplete")
    receipt = evaluate_adjudicated_gold_reliability(gold_event_denominator=1909, cases=cases,
        decisions=decisions, raw_audit_receipt=audit, input_hashes={"lineage": _sha_json(lineage), "audit": _sha_json(audit)})
    out = R / "corrected-review-reconciliation-v1"
    out.mkdir(parents=True, exist_ok=True, mode=0o700)
    immutable_json(out / "decisions.json", decisions)
    immutable_json(out / "lineage.json", lineage)
    immutable_json(out / "reliability.json", receipt)
    print(json.dumps(receipt))


if __name__ == "__main__":
    reconcile()
