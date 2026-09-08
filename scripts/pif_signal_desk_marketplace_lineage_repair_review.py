#!/usr/bin/env python3
"""Review a source-versus-utility need distinction without changing lineage."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from copy import deepcopy
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest

WID='sdw_777d46db3fa4c592b71e';EID='evt_10';OUT=run.OUT/'recovery-need-review-v1'
SYSTEM=run.previous.review.SYSTEM+"""\nIndependently review this explicit repair:
change only evidence_role.needs from wider_context to none. The record remains
substantive_claim, boundary, and uncertain. Its claim explicitly preserves the
unspecified form/direction of the impact. All 11 records and 20 input dispositions
are unchanged. Distinguish a faithful but strategically vague attributed claim
from source text too clipped/garbled/unresolved to establish what was said.
Is the proposed change justified, or should this instead be a quarantined
research_limitation with a source recovery need? Neither route means fabricated
evidence, and no route authorizes publication. Give a precise source-grounded
reason or correction; do not approve merely to satisfy a validator. The source,
not prior reviewers or author labels, is authoritative. Review assigned IDs only.
"""


def prepare(*,write=True):
    import tiktoken
    d=run.OUT/'calls'/WID/'C';saved=json.loads((d/'packet.json').read_text())
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text());authors={}
    for role in ('A','B'):
        parent_packet=run.packet(source,role,{})
        authors[role],_=run.verified_call(run.previous.OUT/'calls'/WID/role,parent_packet,imported=True)
    p=run.packet(source,'C',authors)
    if p!=saved:raise ValueError('reconstructed C packet changed')
    sha=p['packet_sha256']
    raw=json.loads((d/f'{sha}.output.json').read_text());side=json.loads((d/f'{sha}.sidecar.json').read_text());h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    if digest(raw)!='67e20cdb489b70cb648d67fb494d3dd9a6fd7b2bdb995a8056000328e3d5ff5a' or sha!='47996daaa7a0c50a432b6191a2495ac9c3c96f518a5457173e3ff579fec83bda':raise ValueError('not inspected C')
    if (side.get('state')!='completed' or side.get('error_class') or side.get('model')!='gpt-5.6-sol' or side.get('effort')!='medium' or
        side.get('base_instructions_sha256')!=h(run.system('C')) or side.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False))):raise ValueError('C provider provenance changed')
    if digest({k:v for k,v in p.items() if k!='packet_sha256'})!=sha:raise ValueError('C packet changed')
    index=next(i for i,e in enumerate(raw['records']['events']) if e['event_id']==EID)
    repaired_records,proof=propose(raw['records'],source=p['transcript_window'],window_id=WID,expected_original_sha256=digest(raw['records']),replacements=[{
        'path':['events',index,'evidence_role','needs'],'before':'wider_context','after':'none',
        'reason':'The attributed words are intelligible; strategic underspecification remains explicit in claim, rationale and uncertain boundary status rather than being labeled source corruption.'}],
        output_validator=run.previous.contract.validate)
    proposed=deepcopy(raw);proposed['records']=repaired_records;run.validate(proposed,p)
    proof.update(original_envelope_sha256=digest(raw),proposed_envelope_sha256=digest(proposed),
                 lineage_unchanged=proposed['input_dispositions']==raw['input_dispositions'] and proposed['additions']==raw['additions'])
    enc=tiktoken.get_encoding('o200k_base');review=run.previous.review
    ps=review.packets(proposed['records'],source=p['transcript_window'],window_id=WID,token_count=lambda text:len(enc.encode(text)))
    r=next(q for q in ps if any(e['event_id']==EID for e in q['candidates']))
    r.pop('packet_sha256');r['candidates']=[e for e in r['candidates'] if e['event_id']==EID]
    refs={e['voice_binding_id'] for e in r['candidates']};r['voice_bindings']=[b for b in r['voice_bindings'] if b['voice_binding_id'] in refs]
    r['repair_provenance']=proof;r['system_sha256']=digest(SYSTEM);r['schema_sha256']=digest(review.schema(r));r['packet_sha256']=digest(r)
    tokens=len(enc.encode(SYSTEM+json.dumps(r,ensure_ascii=False)+json.dumps(review.schema(r))))+1500
    if tokens>12000:raise ValueError('full source exceeds limit')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for name,v in [('proposal',proposed),('provenance',proof),(r['packet_sha256']+'.packet',r)]:immutable_json(OUT/f'{name}.json',v)
        immutable_json(OUT/'plan.json',{'packets':[r['packet_sha256']],'original_records':11,'input_dispositions':20,
            'input_tokens':tokens,'source_packet_sha256':sha,'sidecar_sha256':digest(side),'applied':False,'gold_accepted':False})
    return [r],proposed,proof


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps,_,_=prepare()
        print(json.dumps({'packets':1,'original_records':11,'input_dispositions':20,'applied':False}),flush=True)
        if args.execute:raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='marketplace-lineage-need-review-v1',system=SYSTEM,
            schema_for_packet=run.previous.review.schema,validator=run.previous.review.validate_review,
            packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
