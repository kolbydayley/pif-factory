"""Review all four fresh roles and C lineage only after the full inventory exists."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_question_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory import signal_desk_question_review as review
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=run.OUT/'final-review'


def collect():
    plan=json.loads((run.OUT/'plan.json').read_text())
    if plan!=run.prepare(write=False):raise ValueError('frozen question plan changed')
    entries={}
    for wid in plan['window_ids']:
        source=run.source_for(plan,wid);outputs={};proofs={}
        for role in run.ROLES:
            p=run.packet(source,role,outputs)
            outputs[role],proofs[role]=run.verified_call(run.OUT/'calls'/wid/role,p)
        entries[wid]={'source':source,'outputs':outputs,'provenance':proofs}
    if len(entries)!=16 or sum(len(e['outputs']) for e in entries.values())!=64:
        raise ValueError('full 64-role inventory required')
    return plan,entries


def prepare(*,write=True):
    import tiktoken
    plan,entries=collect();enc=tiktoken.get_encoding('o200k_base');count=lambda s:len(enc.encode(s))
    record_packets=[];ledger_packets=[];inventory={}
    for wid,entry in entries.items():
        source=entry['source'];outputs=entry['outputs']
        for role in run.ROLES:
            value=outputs[role]['records'] if role=='C' else outputs[role]
            ps=review.packets(value,source=source['transcript_window'],window_id=wid,token_count=count)
            for p in ps:
                p.pop('packet_sha256');p.update(author_role=role,role_output_sha256=digest(outputs[role]));p['packet_sha256']=digest(p)
                if count(review.SYSTEM+json.dumps(p,ensure_ascii=False)+json.dumps(review.schema(p)))+1500>12000:
                    raise ValueError('review metadata exceeds full-source packet budget')
            record_packets.extend(ps)
        ledger_packets.extend(review.ledger_packets(outputs['C'],run.packet(source,'C',outputs),token_count=count))
        inventory[wid]={'outputs':{r:digest(v) for r,v in outputs.items()},'provenance':entry['provenance']}
    frozen={'source_plan_sha256':digest(plan),'windows':16,'role_outputs':64,'inventory':inventory,
            'record_packets':[p['packet_sha256'] for p in record_packets],'ledger_packets':[p['packet_sha256'] for p in ledger_packets],
            'review_contract':review.receipt(),'model':'gpt-5.5','effort':'high','max_concurrency':1,
            'qualified':False,'gold_accepted':False}
    if write:
        for kind,ps in [('records',record_packets),('ledger',ledger_packets)]:
            for p in ps:run.immutable_json(OUT/kind/f"{p['packet_sha256']}.packet.json",p)
        run.immutable_json(OUT/'plan.json',frozen)
    return record_packets,ledger_packets,frozen


async def run_review(records,ledger):
    code=await execute(records,output_root=OUT/'records',task_prefix='question-all-role-final-review-v1',
        system=review.SYSTEM,schema_for_packet=review.schema,validator=review.validate_review,
        packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])
    if code:return code
    return await execute(ledger,output_root=OUT/'ledger',task_prefix='question-lineage-final-review-v1',
        system=review.LEDGER_SYSTEM,schema_for_packet=review.ledger_review.schema,validator=review.ledger_review.validate,
        packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])


def verify_results():
    from collections import Counter
    from research_factory.signal_desk_actual_review_receipt import verify
    records,ledger,expected=prepare(write=False)
    if json.loads((OUT/'plan.json').read_text())!=expected:raise ValueError('final review plan changed')
    rows=[];notes=[];proofs=[]
    for kind,packets,system,validator in [('records',records,review.SYSTEM,review.validate_review),
            ('ledger',ledger,review.LEDGER_SYSTEM,review.ledger_review.validate)]:
        for p in packets:
            value,proof=verify(OUT/kind,p,system=system,validator=validator);proofs.append(proof)
            for decision in value['decisions']:
                rows.append({'kind':kind,'window_id':p['window_id'],'role':p.get('author_role','C'),
                             'id':decision.get('event_id',decision.get('decision_id')),'verdict':decision['verdict']})
            if kind=='records':
                if value['coverage_status']!='no_omissions_found' or value['empty_window_verdict'] in {'missed_records','unusable'}:
                    notes.append({'packet_sha256':p['packet_sha256'],'window_id':p['window_id'],
                                  'role':p['author_role'],'coverage_status':value['coverage_status'],
                                  'requires_source_adjudication':True})
    return {'windows':16,'role_outputs':64,'verdict_counts':dict(Counter(r['verdict'] for r in rows)),
            'all_decisions_supported':all(r['verdict']=='supported' for r in rows),
            'coverage_adjudications_required':notes,'reviewed_packets':len(proofs),'proofs':proofs,
            'qualified':False,'gold_accepted':False,'review_does_not_replace_reliability_audit':True}


if __name__=='__main__':
    parser=argparse.ArgumentParser();group=parser.add_mutually_exclusive_group();group.add_argument('--execute',action='store_true');group.add_argument('--verify',action='store_true');args=parser.parse_args()
    with (run.previous.previous.parent.OUT/'runner.lock').open('a') as v4,(run.previous.previous.OUT/'runner.lock').open('a') as v5:
        for lock in (v4,v5):fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.verify:
            print(json.dumps(verify_results()),flush=True);raise SystemExit(0)
        records,ledger,_=prepare();print(json.dumps({'record_packets':len(records),'ledger_packets':len(ledger),'gold_accepted':False}),flush=True)
        if args.execute:raise SystemExit(asyncio.run(run_review(records,ledger)))
