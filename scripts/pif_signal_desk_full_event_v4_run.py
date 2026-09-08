#!/usr/bin/env python3
"""Isolated v4 all-role development qualification with existing budget controls."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_attribution_probe_run import BASE, freeze, execute as metered_execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_v4 import schema, validate
from research_factory.signal_desk_full_event_v4_prompts import packet, prompts, receipt
from research_factory import signal_desk_full_event_v4_review as final
from research_factory.signal_desk_rubric_reference_packets import digest
from research_factory.signal_desk_provider_schema import lower

ORIGINAL_OUT = BASE.parent / "full-event-semantic-qualification-v4"
OUT = ORIGINAL_OUT / "provider-schema-v1"


def prepare():
    import tiktoken
    freeze()  # Existing development-only source selection, no sealed answers.
    old = json.loads((BASE / "plan.json").read_text())
    if len(old["packet_digests"]) != 16: raise ValueError("full original diagnostic population required")
    sources = [json.loads((BASE / f"{sha}.packet.json").read_text()) for sha in old["packet_digests"]]
    if len({s["window_id"] for s in sources}) != 16: raise ValueError("duplicate development windows")
    independent = [packet(s, role) for s in sources for role in ("A", "B", "AUDIT")]
    enc = tiktoken.get_encoding("o200k_base")
    input_sizes = {p["packet_sha256"]: len(enc.encode(prompts()[p["role"]] + json.dumps(p, ensure_ascii=False) + json.dumps(schema()))) for p in independent}
    # A complete source must fit the final review even before actual records are
    # generated; actual per-record packing is checked again after C completes.
    empty_review_sizes = {}
    from research_factory.signal_desk_full_event_v4 import VERSION
    for s in sources:
        empty = {"schema_version": VERSION, "window_id": s["window_id"], "window_disposition": "no_records", "events": [], "voice_bindings": []}
        ps = final.packets(empty, source=s["transcript_window"], window_id=s["window_id"], token_count=lambda text: len(enc.encode(text)))
        empty_review_sizes[s["window_id"]] = len(enc.encode(final.SYSTEM + json.dumps(ps[0], ensure_ascii=False) + json.dumps(final.schema()))) + 1500
    provider_schema, lowering = lower(schema())
    plan = {"contract": receipt(), "final_review": final.receipt(), "source_plan_sha256": digest(old),
        "provider_schema": lowering,
        "predecessor_plan_sha256": digest(json.loads((ORIGINAL_OUT / "plan.json").read_text())),
        "window_ids": [s["window_id"] for s in sources], "source_packets": {s["window_id"]: s["packet_sha256"] for s in sources},
        "independent_packets": [p["packet_sha256"] for p in independent], "input_token_counts": input_sizes,
        "empty_review_baseline_tokens": empty_review_sizes, "sol_calls": {"A": 16, "B": 16, "C": 16, "AUDIT": 16},
        "model": "gpt-5.6-sol", "reasoning": "medium", "max_concurrency": 2,
        "gold_accepted": False, "qualified": False, "sealed_items_opened": False,
        "scope": "Unchanged16developmentwindows; composition qualification, not tournament/benchmark acceptance."}
    OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for p in independent: immutable_json(OUT / "packets" / f"{p['packet_sha256']}.json", p)
    immutable_json(OUT / "schema.json", schema()); immutable_json(OUT / "plan.json", plan)
    immutable_json(OUT / "provider-schema.json", provider_schema)
    return plan


async def execute(plan, *, output_root=None, packet_builder=packet, prompt_factory=prompts,
                  review_contract=final, task_prefix="full-event-semantic-v4-provider-schema-v1",
                  output_validator=validate, output_schema=schema):
    out = OUT if output_root is None else output_root
    slots = asyncio.Semaphore(2); stop = asyncio.Event()
    # Operational admission control, separate from budget/usage kills. Never
    # cancel a paid call already in flight; re-read before each subsequent role.
    def admission_held():
        return (out / "ADMISSION-HOLD.json").exists()
    async def window(wid):
        async with slots:
            if admission_held(): return {"window_id": wid, "status": "quality_admission_hold"}
            if stop.is_set(): return {"window_id": wid, "status": "not_started_after_failure"}
            s = json.loads((BASE / f"{plan['source_packets'][wid]}.packet.json").read_text()); authored = {}
            for role in ("A", "B", "C", "AUDIT"):
                if admission_held(): return {"window_id": wid, "status": "quality_admission_hold", "role": role}
                if stop.is_set(): return {"window_id": wid, "status": "checkpointed_after_peer_failure"}
                try:
                    p = packet_builder(s, role, author_a=authored.get("A") if role == "C" else None, author_b=authored.get("B") if role == "C" else None)
                    target = out / "calls" / wid / role
                    immutable_json(target / "packet.json", p)
                    code = await metered_execute([p], output_root=target, task_prefix=task_prefix,
                        system_for_packet=lambda q: prompt_factory()[q["role"]], schema_for_packet=lambda q: lower(output_schema())[0],
                        validator=lambda v, q: output_validator(v, source=q["transcript_window"], window_id=q["window_id"]), turn_for_packet=lambda q: q["role"])
                    if code != 0:
                        stop.set(); return {"window_id": wid, "status": "contract_failure", "role": role}
                    authored[role] = output_validator(json.loads((target / f"{p['packet_sha256']}.result.json").read_text()), source=s["transcript_window"], window_id=wid)
                    if role == "C":
                        import tiktoken
                        enc = tiktoken.get_encoding("o200k_base")
                        # No provider review starts here; preflight future full-source
                        # approval limits and preserve all candidates before continuing.
                        reviews = review_contract.packets(authored[role], source=s["transcript_window"], window_id=wid, token_count=lambda text: len(enc.encode(text)))
                        for r in reviews: immutable_json(out / "final-packets" / f"{r['packet_sha256']}.json", r)
                except Exception as exc:
                    stop.set(); return {"window_id": wid, "status": "checkpointed_failure", "role": role,
                        "error_class": type(exc).__name__, "detail": str(exc)[:240]}
            return {"window_id": wid, "status": "authored_not_accepted"}
    rows = await asyncio.gather(*(window(wid) for wid in plan["window_ids"]))
    result = {"complete": all(r["status"] == "authored_not_accepted" for r in rows), "windows": rows,
        "gold_accepted": False, "qualified": False}
    immutable_json(out / "runs" / f"{digest(result)}.json", result)
    print(json.dumps(result), flush=True)
    return 0 if result["complete"] else 2


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true"); args = parser.parse_args()
    plan = prepare()
    print(json.dumps({"windows": len(plan["window_ids"]), "independent_packets": len(plan["independent_packets"]),
        "max_independent_input_tokens": max(plan["input_token_counts"].values()), "max_empty_review_tokens": max(plan["empty_review_baseline_tokens"].values()),
        "gold_accepted": False}), flush=True)
    with (OUT / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.execute:
            immutable_json(OUT / "execution-contract.json", {"plan_sha256": digest(plan), "final_review": final.receipt(),
                "max_concurrency": 2, "scope": plan["scope"], "gold_accepted": False, "qualified": False})
            raise SystemExit(asyncio.run(execute(plan)))


if __name__ == "__main__": main()
