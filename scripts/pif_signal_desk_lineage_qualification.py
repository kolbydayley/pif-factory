#!/usr/bin/env python3
"""Full sixteen-source qualification with explicit C lineage, not gold approval."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_full_event_v5_run as previous
from scripts import pif_signal_desk_lineage_source_review as source_review
from scripts.pif_signal_desk_attribution_probe_run import execute as metered_execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory import signal_desk_adjudication_lineage as lineage
from research_factory.signal_desk_full_event_v5_offset_recovery import load_call
from research_factory.signal_desk_provider_schema import lower
from research_factory.signal_desk_rubric_reference_packets import digest

OUT=previous.OUT.parent/'full-event-lineage-qualification-v1'
ROLES=('A','B','C','AUDIT')


def system(role):return lineage.system() if role=='C' else previous.author.prompts()[role]
def schema(role):return lineage.schema() if role=='C' else previous.contract.schema()
def packet(source,role,outputs):
    return lineage.packet(source,author_a=outputs['A'],author_b=outputs['B']) if role=='C' else previous.author.packet(source,role)
def validate(value,p):
    if p['role']=='C':return lineage.validate(value,source=p['transcript_window'],window_id=p['window_id'],author_a=p['author_a'],author_b=p['author_b'])
    return previous.contract.validate(value,source=p['transcript_window'],window_id=p['window_id'])


def verify_provider(directory,p):
    sha=p['packet_sha256'];side=json.loads((directory/f'{sha}.sidecar.json').read_text())
    if json.loads((directory/'packet.json').read_text())!=p:raise ValueError('saved packet changed')
    h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    if (side.get('state')!='completed' or side.get('error_class') or side.get('model')!='gpt-5.6-sol' or
        side.get('effort')!='medium' or side.get('base_instructions_sha256')!=h(system(p['role'])) or
        side.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False))):raise ValueError('provider provenance mismatch')
    return side


def verified_call(directory,p,*,imported=False):
    side=verify_provider(directory,p);sha=p['packet_sha256']
    if imported:
        if p['role']=='C':raise ValueError('old C cannot be imported')
        value,proof=load_call(directory,p)
    else:
        value=json.loads((directory/f'{sha}.result.json').read_text())
        raw=json.loads((directory/f'{sha}.output.json').read_text())
        proof={'raw_sha256':digest(raw),'result_sha256':digest(value)}
        if raw!=value:
            if (directory/'september8-offset-repair.json').exists():
                from research_factory.signal_desk_september8_offsets import recover,CASES
                case=next(k for k,s in CASES.items() if s['window']==p['window_id'] and s['role']==p['role'])
                expected,repair_proof=recover(case,raw,p);receipt_name='september8-offset-repair.json';proof['inspected_offset_repair']=True
            elif (directory/'hidden-brain-c-repair.json').exists():
                from research_factory.signal_desk_hidden_brain_c_recovery import recover
                expected,repair_proof=recover(raw,p);receipt_name='hidden-brain-c-repair.json';proof['reviewed_hidden_brain_c_repair']=True
            elif (directory/'coding-tools-repair.json').exists():
                from research_factory.signal_desk_coding_tools_final_recovery import recover
                expected,repair_proof=recover(raw,p);receipt_name='coding-tools-repair.json';proof['reviewed_coding_tools_repair']=True
            elif (directory/'inspected-offset-repair.json').exists():
                from research_factory.signal_desk_lineage_audit_offsets import recover
                expected,repair_proof=recover(raw,p);receipt_name='inspected-offset-repair.json';proof['inspected_offset_repair']=True
            else:
                if not (directory/'source-need-repair.json').exists():raise ValueError('new output requires explicit repair provenance')
                from research_factory.signal_desk_reviewed_need_recovery import recover
                case='hidden-brain-b' if p['role']=='B' else 'marketplace'
                expected,repair_proof=recover(case,raw,p);receipt_name='source-need-repair.json';proof['reviewed_source_need_repair']=True
            if expected!=value or json.loads((directory/receipt_name).read_text())!=repair_proof:raise ValueError('explicit repair changed')
            proof['repair_proof_sha256']=digest(repair_proof)
    validate(value,p)
    return value,{'directory':str(directory),'packet_sha256':sha,'sidecar_sha256':digest(side),'imported':imported,**proof}


def prepare():
    old=previous.prepare()
    if len(old['window_ids'])!=16 or len(set(old['window_ids']))!=16:raise ValueError('full original diagnostic required')
    reviewed=source_review.status(source_review.prepare())
    if not reviewed['all_reviews_verified'] or reviewed['total_cases']!=11:raise ValueError('independent source review incomplete')
    plan={'window_ids':old['window_ids'],'source_packets':old['source_packets'],'predecessor_plan_sha256':digest(old),
          'lineage_contract':lineage.receipt(),'independent_role_contract':previous.author.receipt(),
          'source_review_plan_sha256':digest(json.loads((source_review.OUT/'plan.json').read_text())),
          'source_review_receipt_sha256':digest(reviewed),'required_role_outputs':64,'model':'gpt-5.6-sol','effort':'medium',
          'max_concurrency':2,'unchanged_roles_reuse_requires_exact_provenance':True,'qualified':False,'gold_accepted':False}
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    immutable_json(OUT/'plan.json',plan)
    for role in ROLES:immutable_json(OUT/f'{role}.provider-schema.json',lower(schema(role))[0])
    return plan


async def execute(plan):
    slots=asyncio.Semaphore(2);stop=asyncio.Event()
    async def window(wid):
        async with slots:
            outputs={};proofs={}
            for role in ROLES:
                if stop.is_set() or (OUT/'ADMISSION-HOLD.json').exists():
                    return {'window_id':wid,'state':'checkpointed','completed_roles':list(outputs)}
                try:
                    source=json.loads((previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
                    p=packet(source,role,outputs);sha=p['packet_sha256'];target=OUT/'calls'/wid/role
                    old=previous.OUT/'calls'/wid/role
                    if role!='C' and (old/'packet.json').exists():
                        # Existing paid failures are held, not deduped away or
                        # submitted under a new task key. Other independent
                        # windows may proceed; final qualification stays blocked.
                        try:outputs[role],proofs[role]=verified_call(old,p,imported=True)
                        except (ValueError,OSError) as exc:
                            return {'window_id':wid,'state':'held_predecessor','role':role,'reason':str(exc),'completed_roles':list(outputs)}
                        immutable_json(OUT/'imports'/wid/f'{role}.json',proofs[role]);continue
                    immutable_json(target/'packet.json',p)
                    # A completed, invalid response is a held semantic sample,
                    # not a transient retry. Preserve it and its dependent roles
                    # while independent windows advance on an explicit resume.
                    if not (target/f'{sha}.result.json').exists() and (target/f'{sha}.output.json').exists():
                        verify_provider(target,p)
                        raw=json.loads((target/f'{sha}.output.json').read_text())
                        try:validate(raw,p)
                        except ValueError as exc:
                            return {'window_id':wid,'state':'held_existing_call','role':role,
                                    'reason':str(exc),'completed_roles':list(outputs)}
                        raise ValueError('valid raw output missing result; explicit recovery required')
                    if not (target/f'{sha}.result.json').exists():
                        code=await metered_execute([p],output_root=target,task_prefix='full-event-lineage-qualification-v1',
                            system_for_packet=lambda q:system(q['role']),schema_for_packet=lambda q:lower(schema(q['role']))[0],
                            validator=validate,turn_for_packet=lambda q:q['role'])
                        if code!=0:
                            stop.set();return {'window_id':wid,'state':'held_new_call','role':role,'completed_roles':list(outputs)}
                    outputs[role],proofs[role]=verified_call(target,p)
                except Exception as exc:
                    stop.set();return {'window_id':wid,'state':'checkpointed_failure','role':role,'reason':str(exc)[:240],'completed_roles':list(outputs)}
            immutable_json(OUT/'windows'/f'{wid}.json',{'outputs':{r:digest(v) for r,v in outputs.items()},'provenance':proofs,'gold_accepted':False})
            return {'window_id':wid,'state':'authored_not_accepted','completed_roles':list(outputs)}
    rows=await asyncio.gather(*(window(wid) for wid in plan['window_ids']))
    result={'windows':rows,'all_roles_complete':all(r['state']=='authored_not_accepted' for r in rows),
            'expected_windows':16,'expected_role_outputs':64,'qualified':False,'gold_accepted':False}
    immutable_json(OUT/'runs'/f'{digest(result)}.json',result);print(json.dumps(result),flush=True)
    return 0 if result['all_roles_complete'] else 2


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (previous.parent.OUT/'runner.lock').open('a') as v4lock, (previous.OUT/'runner.lock').open('a') as v5lock:
        for lock in (v4lock,v5lock):fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        plan=prepare();print(json.dumps({'windows':16,'role_outputs':64,'gold_accepted':False}),flush=True)
        if args.execute:raise SystemExit(asyncio.run(execute(plan)))


if __name__=='__main__':main()
