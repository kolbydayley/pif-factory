#!/usr/bin/env python3
"""Independent source review of adjudication-merge distinctions; no label edits."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_full_event_v5_run as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory import signal_desk_adjudication_lineage as lineage
from research_factory.signal_desk_full_event_v5_offset_recovery import load_call
from research_factory.signal_desk_rubric_reference_packets import digest

OUT=run.OUT.parent/'adjudication-lineage-source-review-v1'
WID='sdw_777d46db3fa4c592b71e'
SYSTEM=run.author.COMMON+'\n'+lineage.RULES+"""\nIndependently review the assigned
record groups against the FULL source. Neither the grouping nor prior author
labels establishes that a duplicate exists. Decide merge_equivalent,
keep_distinct, split_and_merge, reject_unsupported, or unresolved. Distinguish
duplicate paraphrases from different propositions sharing an excerpt, a
question and its answer, metadata and claims, and source limitations. Provide
exact source quotes and a precise disposition rationale for each assigned case.
Also say whether the proposed lineage contract suffices for this case or needs
a specific rule correction. Do not produce revised gold, self-approve the corpus,
or review unassigned IDs. All original IDs remain in the ledger even if rejected.
"""


def schema(p):
    string={'type':'string','minLength':1}
    fields={'case_id':{'type':'string','enum':[c['case_id'] for c in p['cases']]},
        'verdict':{'type':'string','enum':['merge_equivalent','keep_distinct','split_and_merge','reject_unsupported','unresolved']},
        'rationale':string,'source_quotes':{'type':'array','minItems':1,'items':string},
        'contract_change':{'type':'string'}}
    return {'type':'object','additionalProperties':False,'required':['decisions'],'properties':{
        'decisions':{'type':'array','minItems':len(p['cases']),'maxItems':len(p['cases']),
            'items':{'type':'object','additionalProperties':False,'required':list(fields),'properties':fields}}}}


def validate(v,p):
    if not isinstance(v,dict) or set(v)!={'decisions'} or not isinstance(v['decisions'],list):raise ValueError('invalid review envelope')
    expected={c['case_id'] for c in p['cases']};seen=set();fields=schema(p)['properties']['decisions']['items']['properties']
    for r in v['decisions']:
        if not isinstance(r,dict) or set(r)!=set(fields):raise ValueError('invalid decision fields')
        if r['case_id'] not in expected or r['case_id'] in seen:raise ValueError('unknown or duplicate case')
        seen.add(r['case_id'])
        if r['verdict'] not in fields['verdict']['enum']:raise ValueError('invalid verdict')
        if not isinstance(r['rationale'],str) or not r['rationale'].strip() or not isinstance(r['contract_change'],str):raise ValueError('invalid rationale')
        if not isinstance(r['source_quotes'],list) or not r['source_quotes'] or any(not isinstance(q,str) or not q.strip() or q not in p['transcript_window'] for q in r['source_quotes']):raise ValueError('quote not exact source')
    if seen!=expected:raise ValueError('missing case')
    return v


def prepare():
    import tiktoken
    plan=json.loads((run.OUT/'plan.json').read_text());source=json.loads((run.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text())
    authors={};proofs={}
    for role in ('A','B'):
        p=run.author.packet(source,role);directory=run.OUT/'calls'/WID/role
        if json.loads((directory/'packet.json').read_text())!=p:raise ValueError('source packet mismatch')
        sidecar=json.loads((directory/f"{p['packet_sha256']}.sidecar.json").read_text())
        h=lambda text:hashlib.sha256(text.encode()).hexdigest()
        if (sidecar.get('state')!='completed' or sidecar.get('error_class') or
            sidecar.get('model')!='gpt-5.6-sol' or sidecar.get('effort')!='medium' or
            sidecar.get('base_instructions_sha256')!=h(run.author.prompts()[role]) or
            sidecar.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False))):
            raise ValueError('author sidecar model or request changed')
        authors[role],proofs[role]=load_call(directory,p)
    # Both equivalent and deliberately non-equivalent contrasts; no expected
    # decision is shown to the reviewer. No blind-audit output enters packets.
    groups=[ [('B',f'{WID}_e{i}'),('A',f'evt_{j:02d}')] for i,j in [(1,1),(2,2),(3,3),(4,4),(5,5),(7,9),(8,10),(9,11)] ]
    groups += [[('B',f'{WID}_e6'),('A','evt_06'),('A','evt_07')],
               [('A','evt_07'),('A','evt_08')], [('A','evt_01'),('A','evt_02')]]
    indexed={a:{e['event_id']:e for e in v['events']} for a,v in authors.items()}
    cases=[]
    for i,group in enumerate(groups):
        cases.append({'case_id':f'lineage-{i+1:02d}','records':[{'author':a,'record':indexed[a][eid]} for a,eid in group]})
    enc=tiktoken.get_encoding('o200k_base');packets=[];counts=[]
    for offset in range(0,len(cases),2):
        p={'window_id':WID,'transcript_window':source['transcript_window'],'original_source_packet_sha256':source['packet_sha256'],
           'contract_receipt':lineage.receipt(),'author_provenance':proofs,'cases':cases[offset:offset+2],'system_sha256':digest(SYSTEM)}
        p['schema_sha256']=digest(schema(p));p['packet_sha256']=digest(p)
        tokens=len(enc.encode(SYSTEM+json.dumps(p,ensure_ascii=False)+json.dumps(schema(p))))+1500
        if tokens>12000 or sum(len(c['records']) for c in p['cases'])>25:raise ValueError('untruncated review exceeds packet limits')
        packets.append(p);counts.append(tokens)
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    for p in packets:immutable_json(OUT/f"{p['packet_sha256']}.packet.json",p)
    immutable_json(OUT/'plan.json',{'packets':[p['packet_sha256'] for p in packets],'cases':len(cases),'input_tokens':counts,
        'model':'gpt-5.5','effort':'high','concurrency':1,'contract':lineage.receipt(),'gold_accepted':False,'sealed_items_opened':False})
    return packets


def status(packets, root=OUT):
    rows=[]
    for p in packets:
        sha=p['packet_sha256']; result=root/f'{sha}.review.json'; side=root/f'{sha}.sidecar.json'
        row={'packet_sha256':sha,'assigned_cases':len(p['cases']),'state':'not_started'}
        if side.exists():
            s=json.loads(side.read_text());row['provider_state']=s.get('state');row['state']='in_progress'
        if result.exists():
            try:
                v=validate(json.loads(result.read_text()),p)
                raw=json.loads((root/f'{sha}.output.json').read_text())
                h=lambda text:hashlib.sha256(text.encode()).hexdigest()
                if (not side.exists() or s.get('state')!='completed' or s.get('error_class') or
                    s.get('model')!='gpt-5.5' or s.get('effort')!='high' or
                    s.get('base_instructions_sha256')!=h(SYSTEM) or s.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False)) or raw!=v):
                    raise ValueError('review provenance mismatch')
                row.update(state='reviewed_not_gold',decisions=[{'case_id':r['case_id'],'verdict':r['verdict'],
                    'contract_note_present':bool(r['contract_change'].strip())} for r in v['decisions']])
            except (ValueError,OSError) as exc:row.update(state='held',reason=str(exc))
        elif (root/f'{sha}.pending.json').exists():row['state']='held'
        elif side.exists() and s.get('state') in {'completed','failed'}:row['state']='terminal_without_valid_review'
        rows.append(row)
    return {'packets':rows,'total_cases':sum(r['assigned_cases'] for r in rows),
            'all_reviews_verified':all(r['state']=='reviewed_not_gold' for r in rows),
            'qualified':False,'gold_accepted':False}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');parser.add_argument('--status',action='store_true');args=parser.parse_args()
    if args.status:
        if args.execute:parser.error('status is read-only')
        print(json.dumps(status(prepare()),sort_keys=True));return
    with (run.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        ps=prepare();print(json.dumps({'packets':len(ps),'cases':sum(len(p['cases']) for p in ps),'gold_accepted':False}),flush=True)
        if args.execute:raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='adjudication-lineage-source-review-v1',
            system=SYSTEM,schema_for_packet=schema,validator=validate,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
