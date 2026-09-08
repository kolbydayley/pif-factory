#!/usr/bin/env python3
"""Review an inspected boundary-only A proposal; preserve original paid output."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_full_event_v4_run import OUT as AUTHOR, BASE
from scripts.pif_signal_desk_attribution_probe_review import execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_full_event_v4_prompts import packet
from research_factory import signal_desk_full_event_v4_review as review
from research_factory.signal_desk_rubric_reference_packets import digest

WID = "sdw_777d46db3fa4c592b71e"
OUT = AUTHOR / "explicit-boundary-repair-v1"
SYSTEM = review.SYSTEM + """\nThis is review of a separately preserved repair proposal,
not a new independent author pass. Inspect repair_provenance before/after edits
against the FULL source. Do not rubber-stamp structural validity: determine
whether narrowed answer evidence retains enough question context to support the
claim, and whether removing the next-speaker boundary from continuity_evidence
preserves valid identity and turn boundaries. Review every supplied record using
the normal shared semantic rules. Nothing is automatically applied to gold.
"""


def prepare():
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base")
    plan = json.loads((AUTHOR / "plan.json").read_text())
    source = json.loads((BASE / f"{plan['source_packets'][WID]}.packet.json").read_text())
    p = packet(source, "A"); directory = AUTHOR / "calls" / WID / "A"; sha = p["packet_sha256"]
    if json.loads((directory / "packet.json").read_text()) != p: raise ValueError("source packet changed")
    sidecar = json.loads((directory / f"{sha}.sidecar.json").read_text())
    if sidecar.get("state") != "completed" or sidecar.get("error_class"):
        raise ValueError("original provider call not cleanly completed")
    original = json.loads((directory / f"{sha}.output.json").read_text()); changes = []
    if len(original["events"]) != 12: raise ValueError("not the inspected twelve-record output")
    for index, b in enumerate(original["voice_bindings"]):
        if b["voice_binding_id"] in {"vb_lisa_mae_brunson_turn", "vb_meghan_turn"}:
            evidence = b["continuity_evidence"]
            if len(evidence) != 2 or evidence[0] != b["identity_anchor"] or evidence[1]["start"] <= b["corridor"]["end"]:
                raise ValueError("not the inspected next-speaker-boundary case")
            changes.append({"path": ["voice_bindings", index, "continuity_evidence"], "before": evidence,
                "after": evidence[:1], "reason": "The next speaker label marks the boundary outside this turn; it is not within-voice continuity evidence. Preserve exact corridor and anchor."})
    if len(changes) != 2: raise ValueError("inspected binding population changed")
    index = next(i for i,e in enumerate(original["events"]) if e["event_id"] == WID + "_e10")
    e = original["events"][index]
    if (e["evidence_start"], e["evidence_end"]) != (1506,1580) or source["transcript_window"][1560:1580] != "Brunson: Oh, 1,000%.":
        raise ValueError("not the inspected question-answer excerpt")
    for field, after in (("evidence_start", 1560), ("evidence_text", source["transcript_window"][1560:1580])):
        changes.append({"path": ["events", index, field], "before": e[field], "after": after,
            "reason": "Exclude interviewer speech from Brunson's spoken evidence; the full source still supplies the question for independent meaning review."})
    proposed, provenance = propose(original, source=source["transcript_window"], window_id=WID,
        expected_original_sha256=digest(original), replacements=changes)
    packets = review.packets(proposed, source=source["transcript_window"], window_id=WID, token_count=lambda s: len(enc.encode(s)))
    for p in packets:
        p.pop("packet_sha256"); p["repair_provenance"] = provenance
        p["repair_system_sha256"] = digest(SYSTEM); p["packet_sha256"] = digest(p)
        if len(enc.encode(SYSTEM + json.dumps(p, ensure_ascii=False) + json.dumps(review.schema()))) + 1500 > 12000:
            raise ValueError("repair review exceeds input limit; no truncation")
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    immutable_json(OUT / "proposal.json", proposed); immutable_json(OUT / "provenance.json", provenance)
    for p in packets: immutable_json(OUT / f"{p['packet_sha256']}.packet.json", p)
    immutable_json(OUT / "plan.json", {"packets": [p["packet_sha256"] for p in packets],
        "original_call_packet_sha256": sha, "original_sidecar_sha256": digest(sidecar),
        "provenance_sha256": digest(provenance), "system_sha256": digest(SYSTEM),
        "gold_accepted": False, "qualified": False, "original_first_pass_failure_preserved": True})
    return packets


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    with (AUTHOR / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        packets = prepare()
        print(json.dumps({"packets": len(packets), "records": sum(len(p["candidates"]) for p in packets), "applied": False}), flush=True)
        if args.execute:
            raise SystemExit(asyncio.run(execute(packets, output_root=OUT, task_prefix="full-event-v4-boundary-repair-v1",
                system=SYSTEM, schema_for_packet=lambda p: review.schema(), validator=review.validate_review,
                packet_id=lambda p: p["packet_sha256"], verdict_rows=lambda v: v["decisions"])))


if __name__ == "__main__": main()
