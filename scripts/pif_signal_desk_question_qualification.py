"""Fresh, metered full-population qualification; never accepted gold by itself."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as previous
from research_factory import signal_desk_question_prompts as authors
from research_factory import signal_desk_question_lineage as lineage
from research_factory.signal_desk_rubric_reference_packets import digest
from research_factory.signal_desk_provider_schema import lower
from scripts.pif_signal_desk_attribution_probe_run import execute as metered_execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json

OUT=previous.OUT.parent/'question-all-role-qualification-v1'
ROLES=('A','B','C','AUDIT')


def system(role): return lineage.system() if role=='C' else authors.prompts()[role]
def schema(role): return lineage.schema() if role=='C' else authors.contract.schema()
def packet(source,role,outputs):
    return lineage.packet(source,author_a=outputs['A'],author_b=outputs['B']) if role=='C' else authors.packet(source,role)
def validate(value,p):
    if p['role']=='C':
        return lineage.validate(value,source=p['transcript_window'],window_id=p['window_id'],author_a=p['author_a'],author_b=p['author_b'])
    return authors.contract.validate(value,source=p['transcript_window'],window_id=p['window_id'])


def source_for(plan,wid):
    sha=plan['source_packets'][wid]
    source=json.loads((previous.previous.BASE/f'{sha}.packet.json').read_text())
    if source['window_id']!=wid or source['packet_sha256']!=sha or digest({k:v for k,v in source.items() if k!='packet_sha256'})!=sha:
        raise ValueError('frozen source changed')
    return source


def prepare(*,write=True):
    old=json.loads((previous.OUT/'plan.json').read_text())
    if len(old['window_ids'])!=16 or len(set(old['window_ids']))!=16 or set(old['source_packets'])!=set(old['window_ids']):
        raise ValueError('full original sixteen windows required')
    for wid in old['window_ids']: source_for(old,wid)
    plan={'window_ids':old['window_ids'],'source_packets':old['source_packets'],'source_plan_sha256':digest(old),
          'authors':authors.receipt(),'lineage':lineage.receipt(),'role_outputs_required':64,
          'max_concurrency':2,'model':'gpt-5.6-sol','effort':'medium','old_results_reusable':False,
          'independent_full_source_review_required':True,'qualified':False,'gold_accepted':False}
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700);immutable_json(OUT/'plan.json',plan)
        for role in ROLES:
            immutable_json(OUT/f'{role}.provider-schema.json',lower(schema(role))[0])
    return plan


def verified_call(directory,p):
    sha=p['packet_sha256'];side=json.loads((directory/f'{sha}.sidecar.json').read_text())
    h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    if (json.loads((directory/'packet.json').read_text())!=p or side.get('state')!='completed'
        or side.get('error_class') or side.get('model')!='gpt-5.6-sol' or side.get('effort')!='medium'
        or side.get('base_instructions_sha256')!=h(system(p['role']))
        or side.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False))):
        raise ValueError('question-family provider provenance mismatch')
    value=json.loads((directory/f'{sha}.result.json').read_text())
    raw=json.loads((directory/f'{sha}.output.json').read_text())
    if value!=raw: raise ValueError('projection requires independently reviewed explicit recovery')
    validate(value,p)
    return value,{'packet_sha256':sha,'sidecar_sha256':digest(side),'raw_sha256':digest(raw),'gold_accepted':False}


async def execute(plan):
    if plan!=prepare(write=False): raise ValueError('question-family frozen plan changed')
    slots=asyncio.Semaphore(2);stop=asyncio.Event()
    async def window(wid):
        async with slots:
            outputs={};proofs={}
            for role in ROLES:
                if stop.is_set() or (OUT/'ADMISSION-HOLD.json').exists():
                    return {'window_id':wid,'state':'checkpointed','completed_roles':list(outputs)}
                try:
                    p=packet(source_for(plan,wid),role,outputs);sha=p['packet_sha256'];directory=OUT/'calls'/wid/role
                    immutable_json(directory/'packet.json',p)
                    if not (directory/f'{sha}.result.json').exists():
                        if (directory/f'{sha}.output.json').exists():
                            return {'window_id':wid,'role':role,'state':'held_existing_response','completed_roles':list(outputs)}
                        code=await metered_execute([p],output_root=directory,task_prefix='question-all-role-qualification-v1',
                            system_for_packet=lambda q:system(q['role']),schema_for_packet=lambda q:lower(schema(q['role']))[0],
                            validator=validate,turn_for_packet=lambda q:q['role'])
                        if code!=0:
                            stop.set();return {'window_id':wid,'role':role,'state':'held_new_call','completed_roles':list(outputs)}
                    outputs[role],proofs[role]=verified_call(directory,p)
                except Exception as exc:
                    stop.set();return {'window_id':wid,'role':role,'state':'checkpointed_failure','reason':str(exc)[:240],'completed_roles':list(outputs)}
            immutable_json(OUT/'windows'/f'{wid}.json',{'outputs':{r:digest(v) for r,v in outputs.items()},'provenance':proofs,'gold_accepted':False})
            return {'window_id':wid,'state':'authored_not_accepted','completed_roles':list(outputs)}
    rows=await asyncio.gather(*(window(wid) for wid in plan['window_ids']))
    result={'windows':rows,'expected_windows':16,'expected_role_outputs':64,
            'all_roles_complete':all(r['state']=='authored_not_accepted' for r in rows),'qualified':False,'gold_accepted':False}
    immutable_json(OUT/'runs'/f'{digest(result)}.json',result);print(json.dumps(result),flush=True)
    return 0 if result['all_roles_complete'] else 2


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (previous.previous.parent.OUT/'runner.lock').open('a') as v4,(previous.previous.OUT/'runner.lock').open('a') as v5:
        for lock in (v4,v5):fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        plan=prepare();print(json.dumps({'windows':16,'fresh_role_outputs_required':64,'gold_accepted':False}),flush=True)
        if args.execute: raise SystemExit(asyncio.run(execute(plan)))
