#!/usr/bin/env python3
"""Plan or explicitly execute the nine-call GPT-5.5 speaker lane."""
from __future__ import annotations
import argparse,asyncio,json,sqlite3,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from research_factory.codex_app_server import CodexAppServerClient
from research_factory.signal_desk_gold_wider_speaker import *  # noqa:F403,E402
from research_factory.signal_desk_rebuild_approval import ApprovalBudgetConfig
from research_factory.signal_desk_rebuild_dispatch import acquire_lease,complete_attempt,fail_attempt_semantically,release_attempt_for_retry
from research_factory.util import write_text_atomic
G=ROOT/'work/signal-desk-rebuild/gold-authoring-v2';V5=G/'results/development-attribution-variants/gold-c-full-source-provenance-v5/full';M=ROOT/'work/signal-desk-rebuild/benchmark/partial-manifest.json';B=G/'results/development';PRIVATE=G/'private-gpt55-wider-speaker-v1';PLAN=G/'artifacts/gold-wider-speaker-plan.json';DISPATCH=G/'dispatch-gpt55-wider-speaker-v1.sqlite';DB=ROOT/'data/factory.sqlite';GRANT=ROOT/'config/signal_desk_rebuild_budget_grant.json';BUDGET=ROOT/'work/pif-ops/budget'
def prepare():
 plan=build_private_packets(v5_root=V5,manifest_path=M,baseline_root=B,project_root=ROOT,private_root=PRIVATE);PLAN.parent.mkdir(parents=True,exist_ok=True);PLAN.write_text(json.dumps(plan,indent=2,sort_keys=True)+'\n');enqueue_packets(dispatch_path=DISPATCH,plan=plan,private_root=PRIVATE);return plan
async def run(args,plan):
 async def worker(index):
  owner=f'wider-speaker-{index}-{uuid.uuid4().hex[:8]}';dispatch=sqlite3.connect(DISPATCH);dispatch.row_factory=sqlite3.Row;budget_db=sqlite3.connect(DB);budget_db.row_factory=sqlite3.Row
  budget=ApprovalBudgetConfig(campaign_id=CAMPAIGN_ID,grant_path=GRANT,budget_dir=BUDGET)
  consecutive_infra_failures=0
  try:
   async with CodexAppServerClient(command=[args.binary,'app-server','--stdio','--strict-config'],expected_cli_version='0.147.0',synthetic_debug_errors=True) as client:
    while True:
     lease=acquire_lease(dispatch,lease_owner=owner,lease_seconds=LEASE_SECONDS,task_key_prefix=TASK_PREFIX)
     if lease is None:break
     payload=lease['payload'];packet_path=Path(payload['packet_path']);packet=json.loads(packet_path.read_text())
     out=PRIVATE/f"{payload['window_id']}.decision.json";side=PRIVATE/f"{payload['window_id']}.attempt-{lease['attempt_number']}.generation-{lease['lease_generation']}.sidecar.json";pending=out.with_suffix('.pending.json');reservation=None
     try:
      if packet['packet_sha256']!=payload['packet_sha256']:raise WiderSpeakerError('packet digest mismatch')
      if out.exists():
       recovered=validate_decision(json.loads(out.read_text()),packet);complete_attempt(dispatch,attempt_id=lease['current_attempt_id'],lease_owner=owner,lease_generation=lease['lease_generation'],output={'window_id':payload['window_id'],'decision_sha256':sha256_text(out.read_text()),'recovered_after_write':True});continue
      reservation_key=lease['task_key']+f":attempt:{lease['attempt_number']}:generation:{lease['lease_generation']}"
      reservation=reserve_call(budget_db,task_key=reservation_key,budget=budget)
      schema=json.loads(json.dumps(OUTPUT_SCHEMA));schema['properties']['packet_sha256']={'type':'string','const':packet['packet_sha256']}
      result=await client.run_ephemeral_structured_turn(model=MODEL,effort=EFFORT,base_instructions=SYSTEM_PROMPT,prompt=json.dumps({'packet_sha256':packet['packet_sha256'],'candidate':packet['candidate'],'context':packet['context']},ensure_ascii=False,separators=(',',':')),output_schema=schema,cwd=ROOT,sidecar_path=side,output_path=pending,timeout_seconds=CALL_DEADLINE_SECONDS)
      tokens=result.usage.total_tokens if result.usage else None;settle_call(budget_db,reservation_id=reservation['reservation_id'],task_key=reservation_key,actual_tokens=tokens)
      if not result.status_ok or result.output is None:raise RuntimeError(result.error_class or result.status)
      decision=validate_decision(result.output,packet)
      if out.exists():raise RuntimeError('immutable decision output already exists')
      write_text_atomic(out,json.dumps(decision,indent=2,sort_keys=True)+'\n');complete_attempt(dispatch,attempt_id=lease['current_attempt_id'],lease_owner=owner,lease_generation=lease['lease_generation'],output={'window_id':payload['window_id'],'decision_sha256':sha256_text(out.read_text())});consecutive_infra_failures=0
     except WiderSpeakerError as exc:
      if reservation and budget_db.execute("select status from signal_desk_wider_speaker_reservations where reservation_id=?",(reservation['reservation_id'],)).fetchone()[0]=='reserved':settle_call(budget_db,reservation_id=reservation['reservation_id'],task_key=reservation_key,actual_tokens=None)
      fail_attempt_semantically(dispatch,attempt_id=lease['current_attempt_id'],lease_owner=owner,lease_generation=lease['lease_generation'],failure_code='speaker_decision_invalid',failure_detail=str(exc)[:300])
     except Exception as exc:
      if reservation and budget_db.execute("select status from signal_desk_wider_speaker_reservations where reservation_id=?",(reservation['reservation_id'],)).fetchone()[0]=='reserved':settle_call(budget_db,reservation_id=reservation['reservation_id'],task_key=reservation_key,actual_tokens=None)
      release_attempt_for_retry(dispatch,attempt_id=lease['current_attempt_id'],lease_owner=owner,lease_generation=lease['lease_generation'],failure_code='provider_or_transport_failure',failure_detail=f'{type(exc).__name__}: {str(exc)[:250]}');consecutive_infra_failures+=1;await asyncio.sleep(min(300,30*(2**(consecutive_infra_failures-1))))
      if consecutive_infra_failures>=3:break
     finally:pending.unlink(missing_ok=True)
  finally:dispatch.close();budget_db.close()
 await asyncio.gather(*(worker(i) for i in range(args.concurrency)))
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute',action='store_true');p.add_argument('--concurrency',type=int,choices=(1,2),default=1);p.add_argument('--binary',default='codex');a=p.parse_args();plan=prepare()
 print(json.dumps({'status':'planned' if not a.execute else 'launching','provider_calls_started':a.execute,'model':MODEL,'expected_calls':plan['expected_calls'],'reserved_token_ceiling':plan['reserved_token_ceiling'],'concurrency':a.concurrency,'launch_command':'python3 -B scripts/pif_signal_desk_gold_wider_speaker.py --execute --concurrency 1'},indent=2,sort_keys=True))
 if a.execute:asyncio.run(run(a,plan))
 return 0
if __name__=='__main__':raise SystemExit(main())
