"""One capacity retry with unchanged request, separate metering and lineage."""
import argparse
import asyncio
from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_coding_tools_b_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_rubric_reference_packets import digest

SHA='67d0f43b3fe744c4f03a0e78bf1ec3863f26baa9fc7f0c86527615307d95f358'
OUT=parent.OUT/'capacity-retry-1'
SYSTEM=parent.SYSTEM


def prepare(*,write=True):
    ps=parent.prepare(write=False)
    if ps[-1]['packet_sha256']!=SHA:raise ValueError('failed packet changed')
    h=lambda text:hashlib.sha256(text.encode()).hexdigest()
    prior=[]
    for p in ps:
        sha=p['packet_sha256'];s=json.loads((parent.OUT/f'{sha}.sidecar.json').read_text())
        if s.get('model')!='gpt-5.5' or s.get('effort')!='high' or s.get('base_instructions_sha256')!=h(SYSTEM) or s.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False)):
            raise ValueError('original provider/request mismatch')
        if json.loads((parent.OUT/f'{sha}.packet.json').read_text())!=p:raise ValueError('original saved packet changed')
        if sha!=SHA:
            if s.get('state')!='completed' or s.get('error_class'):raise ValueError('predecessor review not complete')
            v=json.loads((parent.OUT/f'{sha}.review.json').read_text())
            if v!=json.loads((parent.OUT/f'{sha}.output.json').read_text()):raise ValueError('predecessor review changed')
            parent.run.previous.review.validate_review(v,p)
            prior.append({'packet_sha256':sha,'sidecar_sha256':digest(s),'review_sha256':digest(v)})
        else:
            if s.get('state')!='failed' or s.get('turn_error',{}).get('codex_error_info')!='serverOverloaded' or s.get('turn_error',{}).get('backend_message')!='Selected model is at capacity. Please try a different model.' or s.get('output_sha256') is not None:
                raise ValueError('not the recorded no-output capacity failure')
            if (parent.OUT/f'{sha}.output.json').exists() or (parent.OUT/f'{sha}.review.json').exists():raise ValueError('failed packet now has output; inspect before retry')
            failure=s
    receipt={'packets':[SHA],'parent_packets':[p['packet_sha256'] for p in ps],
        'preserved_reviews':prior,'original_failure_sha256':digest(failure),
        'not_before_epoch':datetime.fromisoformat(failure['finished_at']).timestamp()+300,
        'semantic_sample_unchanged':True,'original_charge_preserved':True,
        'retry_attempt':1,'model':'gpt-5.5','effort':'high','gold_accepted':False}
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        parent.run.immutable_json(OUT/'plan.json',receipt)
        parent.run.immutable_json(OUT/f'{SHA}.packet.json',ps[-1])
    return [ps[-1]],receipt


async def run():
    ps,receipt=prepare()
    delay=max(0,receipt['not_before_epoch']-datetime.now(timezone.utc).timestamp())
    print(json.dumps({'capacity_cooldown_seconds':round(delay),'retry_packets':1,'preserved_completed_packets':3}),flush=True)
    if delay:await asyncio.sleep(delay)
    review=parent.run.previous.review
    return await execute(ps,output_root=OUT,task_prefix='coding-tools-b-source-diagnosis-v1-capacity-attempt-1',
        system=SYSTEM,schema_for_packet=review.schema,validator=review.validate_review,
        packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.execute:raise SystemExit(asyncio.run(run()))
        _,receipt=prepare();print(json.dumps(receipt))


if __name__=='__main__':main()
