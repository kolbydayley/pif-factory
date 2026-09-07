#!/usr/bin/env python3
"""Bounded window-pipelined full-event qualification, isolated from frozen gold."""
import argparse
import asyncio
import fcntl
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_full_event_qualification import prepare, OUT, BASE
from scripts.pif_signal_desk_attribution_probe_run import execute as metered_execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_prompts import packet, prompts
from research_factory.signal_desk_full_event_experiment import schema, validate


async def execute(plan):
    slots = asyncio.Semaphore(2); stop = asyncio.Event()
    async def window(wid):
        async with slots:
            if stop.is_set(): return {"window_id": wid, "status": "not_started_after_failure"}
            source_sha = plan["source_packets"][wid]
            source = json.loads((BASE / f"{source_sha}.packet.json").read_text())
            authored = {}
            for role in ("A", "B", "C", "AUDIT"):
                if stop.is_set(): return {"window_id": wid, "status": "checkpointed_after_peer_failure"}
                value = packet(source, role, author_a=authored.get("A") if role == "C" else None,
                    author_b=authored.get("B") if role == "C" else None)
                target = OUT / "calls" / wid / role
                immutable_json(target / "packet.json", value)
                try:
                    code = await metered_execute([value], output_root=target,
                        task_prefix="full-event-qualification-v3",
                        system_for_packet=lambda p: prompts()[p["role"]],
                        schema_for_packet=lambda p: schema(),
                        validator=lambda v, p: validate(v, source=p["transcript_window"], window_id=p["window_id"]),
                        turn_for_packet=lambda p: p["role"])
                    if code != 0:
                        stop.set(); return {"window_id": wid, "status": "contract_failure", "role": role}
                    authored[role] = validate(json.loads((target / f"{value['packet_sha256']}.result.json").read_text()),
                        source=source["transcript_window"], window_id=wid)
                except Exception as exc:
                    stop.set()
                    return {"window_id": wid, "status": "checkpointed_failure", "role": role, "error_class": type(exc).__name__, "detail": str(exc)[:200]}
            return {"window_id": wid, "status": "authored_not_accepted"}
    # A failure stops new admissions, but other in-flight work drains cleanly.
    rows = await asyncio.gather(*(window(wid) for wid in plan["window_ids"]))
    complete = all(r["status"] == "authored_not_accepted" for r in rows)
    from research_factory.signal_desk_rubric_reference_packets import digest
    result = {"complete": complete, "windows": rows, "qualified": False, "gold_accepted": False}
    immutable_json(OUT / "runs" / f"{digest(result)}.json", result)
    print(json.dumps(result), flush=True)
    return 0 if complete else 2


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    plan = prepare()
    with (OUT / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.execute:
            from research_factory.signal_desk_full_event_review import SYSTEM as final_system, schema as final_schema
            from research_factory.signal_desk_rubric_reference_packets import digest
            immutable_json(OUT / "execution-contract.json", {
                "frozen_plan_sha256": digest(plan), "schema_sha256": plan["contract"]["schema_sha256"],
                "final_system_sha256": digest(final_system), "final_schema_sha256": digest(final_schema()),
                "execution_ready": True, "max_concurrency": 2, "scope": "16-development-window-qualification-only",
                "gold_accepted": False, "qualified": False,
                "runtime_test_receipt": "d1fdb5e: 29 focused tests passed before launch"})
            raise SystemExit(asyncio.run(execute(plan)))


if __name__ == "__main__": main()
