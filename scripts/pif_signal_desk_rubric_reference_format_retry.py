#!/usr/bin/env python3
"""Bounded format-contract retry; preserve first-pass qualification failures."""
import argparse
import asyncio
import fcntl
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_rubric_reference_review as runner
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_rubric_reference_packets import digest

FORMAT_CONTRACT = """
RESPONSE TRANSPORT CONTRACT (does not change the semantic rubric):
correction_json is a JSON object encoded as a string. Its ONLY permitted keys are
speaker_id, attribution_type, quoted_person_id, mentioned_person_ids, stance.
Never put claim_text, evidence, issue, speech_act, reason, note, entity_issue,
overstatement, or any other key in correction_json. Put concerns outside this
field scope in source_basis and retain needs_correction (with correction_json
equal to '{}' if no permitted field correction applies). This does NOT mean the
event is supported. No claim or evidence edits are allowed. Explain the concern
without inventing an unsupported field. supported always requires '{}'.
"""


def prepare():
    original = runner.OUT
    packets = runner.prepare()
    prior = json.loads((original / "receipt.json").read_text())
    pending = prior["pending_packets"]
    if not pending or len(set(pending)) != len(pending):
        raise ValueError("retry requires unique explicitly pending first-pass packets")
    by_sha = {p["packet_sha256"]: p for p in packets}
    if not set(pending) <= set(by_sha):
        raise ValueError("retry packet outside frozen inventory")
    system = runner.REFERENCE_SYSTEM + FORMAT_CONTRACT
    out = original / "format-retry-v1"
    rebuilt = []; bindings = []
    import tiktoken
    enc = tiktoken.get_encoding("o200k_base")
    for sha in pending:
        failure = json.loads((original / f"{sha}.pending.json").read_text())
        if failure["reason"] != "reference correction exceeds field scope":
            raise ValueError("retry does not cover this failure family")
        packet = dict(by_sha[sha]); packet.pop("packet_sha256")
        packet["system_sha256"] = digest(system)
        if len(enc.encode(system + json.dumps(packet, ensure_ascii=False))) + 1500 > 12000:
            raise ValueError("format retry exceeds unchanged token limit")
        packet["packet_sha256"] = digest(packet)
        bindings.append({"original_packet_sha256": sha, "retry_packet_sha256": packet["packet_sha256"],
            "original_output_sha256": digest(json.loads((original / f"{sha}.output.json").read_text()))})
        rebuilt.append(packet)
    receipt = {"family": "reference-response-format-v1", "model": "gpt-5.5", "effort": "high",
        "concurrency": 1, "system_sha256": digest(system), "bindings": bindings,
        "first_pass_total_packets": len(packets), "first_pass_invalid_packets": len(pending),
        "semantic_rubric_changed": False, "gold_accepted": False, "rubric_qualified": False,
        "retry_results_do_not_erase_first_pass_failures": True}
    immutable_json(out / "plan.json", receipt)
    for packet in rebuilt: immutable_json(out / f"{packet['packet_sha256']}.packet.json", packet)
    return out, system, rebuilt


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    with (runner.QUAL / "reference-format-retry.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        out, system, packets = prepare()
        print(json.dumps({"packets": len(packets), "gold_accepted": False}), flush=True)
        if args.execute:
            runner.OUT = out
            runner.REFERENCE_SYSTEM = system
            raise SystemExit(asyncio.run(runner.execute(packets)))


if __name__ == "__main__":
    main()
