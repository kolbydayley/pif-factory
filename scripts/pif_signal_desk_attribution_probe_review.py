#!/usr/bin/env python3
"""Bounded, metered GPT-5.5 review of the complete experimental SOL probe."""
import argparse
import asyncio
from collections import Counter
import fcntl
import json
import sqlite3
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_attribution_probe_run import BASE, OUT as SOL, freeze
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_attribution_contrasts import validate_response
from research_factory.signal_desk_attribution_probe_review import REVIEW_SYSTEM, schema, validate_review, build_packet
from research_factory.signal_desk_rubric_reference_packets import digest
from research_factory.codex_app_server import CodexAppServerClient
from research_factory.signal_desk_gold_wider_speaker import reserve_call, settle_call
from research_factory.signal_desk_rebuild_approval import ApprovalBudgetConfig
OUT = BASE / "gpt55-review-v1"


def prepare():
    import tiktoken
    freeze()
    plan = json.loads((BASE / "plan.json").read_text())
    if len(plan["packet_digests"]) != 16:
        raise ValueError("complete 16-window diagnostic required")
    enc = tiktoken.get_encoding("o200k_base")
    packets = []
    for sha in plan["packet_digests"]:
        packet = json.loads((BASE / f"{sha}.packet.json").read_text())
        if digest({k: v for k, v in packet.items() if k != "packet_sha256"}) != sha:
            raise ValueError("source packet digest mismatch")
        proposed = validate_response(json.loads((SOL / f"{sha}.result.json").read_text()), packet)
        review = build_packet(packet, proposed)
        tokens = len(enc.encode(REVIEW_SYSTEM + json.dumps(review, ensure_ascii=False) + json.dumps(schema(packet)))) + 1500
        if len(packet["anchors"]) > 25 or tokens > 12000:
            raise ValueError("approval packet exceeds limits; no source truncation allowed")
        packets.append(review)
    # Do not freeze or dispatch a partial set if any SOL output is absent/invalid.
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for p in packets: immutable_json(OUT / f"{p['review_packet_sha256']}.packet.json", p)
    immutable_json(OUT / "plan.json", {"packets": [p["review_packet_sha256"] for p in packets],
        "model": "gpt-5.5", "effort": "high", "concurrency": 1,
        "system_sha256": digest(REVIEW_SYSTEM), "max_candidates": 25, "max_input_tokens": 12000,
        "qualified": False, "gold_accepted": False})
    return packets


async def execute(packets):
    db = sqlite3.connect(ROOT / "data/factory.sqlite", timeout=30)
    budget = ApprovalBudgetConfig(campaign_id="signal-desk-clean-corpus-2026-08-31",
        grant_path=ROOT / "config/signal_desk_rebuild_budget_grant.json", budget_dir=ROOT / "work/pif-ops/budget")
    try:
        async with CodexAppServerClient(command=["codex", "app-server", "--stdio", "--strict-config"], expected_cli_version="0.147.0") as client:
            for packet in packets:
                sha = packet["review_packet_sha256"]; original = packet["original_packet"]; proposed = packet["proposed_labels"]
                target = OUT / f"{sha}.review.json"
                if target.exists():
                    validate_review(json.loads(target.read_text()), original, proposed); continue
                if (OUT / f"{sha}.pending.json").exists(): continue
                key = "attribution-probe-gpt55-review-v1:" + sha
                reservation = reserve_call(db, task_key=key, budget=budget)
                if reservation.get("existing"):
                    raise RuntimeError("existing paid review requires sidecar recovery; no blind redispatch")
                usage = None
                try:
                    result = await client.run_ephemeral_structured_turn(model="gpt-5.5", effort="high", base_instructions=REVIEW_SYSTEM,
                        prompt=json.dumps(packet, ensure_ascii=False), output_schema=schema(original), cwd=ROOT,
                        sidecar_path=OUT / f"{sha}.sidecar.json", output_path=OUT / f"{sha}.output.json", timeout_seconds=900)
                    usage = result.usage.total_tokens if result.usage else None
                    if not result.status_ok or result.output is None:
                        raise RuntimeError(result.error_class or result.status)
                    try: validate_review(result.output, original, proposed)
                    except ValueError as exc:
                        immutable_json(OUT / f"{sha}.pending.json", {"packet_sha256": sha, "reason": str(exc), "gold_accepted": False})
                        continue
                    immutable_json(target, result.output)
                finally:
                    # Missing accounting conservatively charges the reservation, never zero.
                    settle_call(db, reservation_id=reservation["reservation_id"], task_key=key, actual_tokens=usage)
    finally: db.close()
    counts = Counter(); pending = []
    for packet in packets:
        sha = packet["review_packet_sha256"]; path = OUT / f"{sha}.review.json"
        if not path.exists(): pending.append(sha); continue
        value = validate_review(json.loads(path.read_text()), packet["original_packet"], packet["proposed_labels"])
        counts.update(row["verdict"] for row in value["reviews"])
    receipt = {"all_calls_validated": not pending, "verdicts": dict(counts), "pending_packets": pending,
        "qualified": False, "gold_accepted": False, "next": "Inspect source-bound corrections and unresolved rows; no automatic promotion"}
    immutable_json(OUT / "receipt.json", receipt)
    print(json.dumps(receipt), flush=True)
    return 0 if not pending else 2


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    BASE.mkdir(parents=True, exist_ok=True)
    with (BASE / "gpt55-review.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        packets = prepare()
        print(json.dumps({"packets": len(packets), "qualified": False}), flush=True)
        if args.execute: raise SystemExit(asyncio.run(execute(packets)))


if __name__ == "__main__": main()
