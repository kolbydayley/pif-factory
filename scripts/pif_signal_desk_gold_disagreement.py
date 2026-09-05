#!/usr/bin/env python3
"""Run the resumable, metered GPT-5.5 development disagreement lane."""
from __future__ import annotations
import argparse, asyncio, hashlib, json, sqlite3, sys
from datetime import datetime, timezone
from pathlib import Path
PIF_ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(PIF_ROOT))
from research_factory.codex_app_server import CodexAppServerClient
from research_factory.signal_desk_gold_audit import select_dev_audit_windows
from research_factory.signal_desk_gold_disagreement import build_batches, build_cases, validate_decisions, MODEL
from research_factory.signal_desk_rebuild_budget import rebuild_budget_gate
from research_factory.subscription_budget import record_usage, subscription_budget_window
from research_factory.util import now_iso, write_text_atomic

ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"; PRIVATE = ROOT / "private-gpt55-disagreement-review"
DB = PIF_ROOT / "data/factory.sqlite"; GRANT = PIF_ROOT / "config/signal_desk_rebuild_budget_grant.json"; BUDGET = PIF_ROOT / "work/pif-ops/budget"
SYSTEM = """You are the final GPT-5.5 authority for a private development Gold-C disagreement audit. Re-read the entire supplied frozen transcript window. For each case, decide whether the Gold-C event, the independent audit event, both, neither, or neither reliably supported proposition is supported by the transcript and the evidence offsets. Treat equivalent wording as both_supported. Do not use outside knowledge. Return one decision for every case ID. correction_json must be an empty string when no correction is needed, or a compact JSON object encoded as a string containing only disputed field names and corrected values; never invent evidence text or offsets. Attest model gpt-5.5."""
SCHEMA = {"type":"object","additionalProperties":False,"required":["model","decisions"],"properties":{"model":{"type":"string","const":MODEL},"decisions":{"type":"array","items":{"type":"object","additionalProperties":False,"required":["case_id","decision","rationale","correction_json"],"properties":{"case_id":{"type":"string"},"decision":{"type":"string","enum":["gold_supported","audit_supported","both_supported","neither_supported","uncertain"]},"rationale":{"type":"string","minLength":1},"correction_json":{"type":"string"}}}}}}
def gate(run_id, tokens=None):
    now=datetime.now(timezone.utc); day, start=subscription_budget_window(now); c=sqlite3.connect(DB, timeout=30)
    try:
        c.execute("PRAGMA busy_timeout=30000")
        if tokens is None:
            x=dict(rebuild_budget_gate(c, day=day, campaign_id="signal-desk-clean-corpus-2026-08-31", grant_path=GRANT, budget_dir=BUDGET, at=now, window_start_iso=start)); c.commit(); return x
        x=record_usage(c, day=day, provider_lane="openai-codex/gpt-5.5", lane="signal_desk_rebuild_disagreement", run_id=run_id, tokens=tokens, provider_calls=1); c.commit(); return {"ledger_id":x,"tokens":tokens}
    finally: c.close()
async def main_async(args):
    launch_stamp=now_iso().replace("-","").replace(":","").replace("+00:00","Z").replace(".","")
    manifest_path=PIF_ROOT/"work/signal-desk-rebuild/benchmark/partial-manifest.json"; manifest=json.loads(manifest_path.read_text()); dev=[str(r["window_id"]) for r in manifest["windows"] if r["split"]=="development"]
    audit=json.loads((ROOT/"artifacts/gold-audit-dev.json").read_text()); selected=list(audit["audit_selection"]["window_ids"])
    cases=build_cases(manifest_path=manifest_path,result_root=ROOT/"results/development",project_root=PIF_ROOT,selected_window_ids=selected)
    if len(cases) != int(audit["critical_errors"]["errors"]): raise RuntimeError(f"disagreement packet drifted: {len(cases)} cases")
    texts={str(r["window_id"]): (PIF_ROOT/str(r["transcript_path"])).read_text(encoding="utf-8")[int(r["start_char"]):int(r["end_char"])] for r in manifest["windows"] if r["window_id"] in selected}
    wider={}
    for r in manifest["windows"]:
        if r["window_id"] not in selected: continue
        raw=(PIF_ROOT/str(r["transcript_path"])).read_text(encoding="utf-8")
        start,end=int(r["start_char"]),int(r["end_char"])
        wider[str(r["window_id"])] = raw[max(0,start-8000):min(len(raw),end+8000)]
    batches=build_batches(cases=cases,window_texts=texts); PRIVATE.mkdir(parents=True,exist_ok=True); PRIVATE.chmod(0o700); q=asyncio.Queue()
    for b in batches:
        out=PRIVATE/f"{b['window_id']}-{b['input_sha256'][:12]}.output.json"
        if not out.exists(): q.put_nowait((b,out))
    async def worker(i):
        async with CodexAppServerClient(command=[args.binary,"app-server","--stdio","--strict-config"],expected_cli_version="0.147.0",synthetic_debug_errors=True) as client:
            while not q.empty():
                b,out=await q.get(); pending=out.with_suffix('.pending.json'); rid=f"gold-disagreement:{b['input_sha256']}"
                try:
                    last_error=None
                    for attempt in (1, 2):
                        side=out.with_name(out.stem+f'.{launch_stamp}.attempt-{attempt}.sidecar.json')
                        attempt_rid=f"{rid}:attempt:{attempt}"
                        if not gate(attempt_rid).get("allowed"): raise RuntimeError("approval budget denied")
                        request=dict(b)
                        if attempt == 2:
                            request["context_mode"]="wider_retry"
                            request["wider_transcript_context"]=wider[b["window_id"]]
                        try:
                            res=await client.run_ephemeral_structured_turn(model=MODEL,effort="high",base_instructions=SYSTEM,prompt="Adjudicate this batch; on retry use the wider context to resolve any ambiguity:\n"+json.dumps(request,ensure_ascii=False,separators=(",",":")),output_schema=SCHEMA,cwd=PIF_ROOT,sidecar_path=side,output_path=pending,timeout_seconds=900)
                            if not res.status_ok or res.output is None or res.usage is None: raise RuntimeError(f"GPT-5.5 failure: {res.error_class or res.status}")
                            validate_decisions(res.output,[str(c["case_id"]) for c in b["cases"]]); gate(attempt_rid,res.usage.total_tokens); write_text_atomic(out,json.dumps(res.output,indent=2,sort_keys=True)+"\n"); last_error=None; break
                        except Exception as exc:
                            last_error=exc
                            pending.unlink(missing_ok=True)
                            if attempt == 1:
                                await asyncio.sleep(30)
                    if last_error is not None: raise last_error
                finally: pending.unlink(missing_ok=True); q.task_done()
    await asyncio.gather(*(worker(i) for i in range(args.concurrency))); decisions={}
    for b in batches:
        out=next(PRIVATE.glob(f"{b['window_id']}-{b['input_sha256'][:12]}.output.json")); decisions.update(validate_decisions(json.loads(out.read_text()),[str(c["case_id"]) for c in b["cases"]]))
    receipt={"schema_version":"pif_signal_desk_gold_disagreement_receipt_v1","created_at":now_iso(),"model":MODEL,"selected_windows":len(selected),"case_count":len(cases),"batch_count":len(batches),"resolved_count":len(decisions),"complete":len(decisions)==len(cases),"decision_hash":hashlib.sha256(json.dumps(decisions,sort_keys=True,separators=(",",":")).encode()).hexdigest(),"privacy":"decisions and rationales remain private; receipt contains counts and hashes only"}
    receipt["receipt_sha256"]=hashlib.sha256(json.dumps(receipt,sort_keys=True,separators=(",",":")).encode()).hexdigest(); (ROOT/"artifacts/gold-disagreement-review-dev.json").write_text(json.dumps(receipt,indent=2,sort_keys=True)+"\n"); print(json.dumps(receipt,indent=2)); return 0 if receipt["complete"] else 2
def main():
    p=argparse.ArgumentParser(); p.add_argument("--concurrency",type=int,choices=(1,2),default=1); p.add_argument("--binary",default="codex"); return asyncio.run(main_async(p.parse_args()))
if __name__=="__main__": raise SystemExit(main())
