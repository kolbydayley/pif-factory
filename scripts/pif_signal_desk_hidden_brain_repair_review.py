#!/usr/bin/env python3
"""Source-bound review of one recovery-need correction; no automatic application."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_full_event_v5_run as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest

WID='sdw_99a1771e94fa2b923f9e';EID=WID+'_e2'
OUT=run.OUT.parent/'hidden-brain-recovery-need-review-v1'
SYSTEM=run.review.SYSTEM+"""\nReview one explicit field repair against the full
source: change evidence_role.needs from wider_context to none for the attributed
prevalence estimate. Everything else, including uncertain publication state,
qualifiers, original rationale and the complete 11-record population, is preserved.
The provenance also records inspected numeric-only span fixes and alignment of
six identity anchors with their existing name-only attribution spans by removing
the trailing label colon. Speaker names and semantic assignments are unchanged.
Distinguish absent study methods or external verification from source context
needed to understand what the speaker actually claimed. Neither factual truth
nor publishability follows from byte grounding. Decide if this particular repair
is justified, or state a specific correction/unresolved need. Do not mechanically
approve it because it satisfies the validator. Review assigned candidates only.
"""


def prepare(*,write=True):
    import tiktoken
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text());p=run.author.packet(source,'A')
    d=run.OUT/'calls'/WID/'A';sha=p['packet_sha256']
    if json.loads((d/'packet.json').read_text())!=p:raise ValueError('source packet changed')
    import hashlib
    h=lambda s:hashlib.sha256(s.encode()).hexdigest();side=json.loads((d/f'{sha}.sidecar.json').read_text())
    if (side.get('state')!='completed' or side.get('error_class') or side.get('model')!='gpt-5.6-sol' or side.get('effort')!='medium' or
        side.get('base_instructions_sha256')!=h(run.author.prompts()['A']) or side.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False))):raise ValueError('original author provenance changed')
    original=json.loads((d/f'{sha}.output.json').read_text());index=next(i for i,e in enumerate(original['events']) if e['event_id']==EID)
    if digest(original)!='b75ae5aa306ce581cdc8d90101f4cd0950344013fe9df9de8525fb214dc53a5a' or sha!='941eef8598172debcdcc36c447f0d990cdfb69e573df28acf337776b1825aa40':raise ValueError('not inspected original response')
    e=original['events'][index]
    if len(original['events'])!=11 or e['publishability_state']!='uncertain' or e['evidence_role']['role']!='substantive_claim':raise ValueError('not inspected record')
    changes=[{'path':['events',index,'evidence_role','needs'],'before':'wider_context','after':'none',
              'reason':'Specific attributed estimate is intelligible in this source; lack of study methodology is a verification limitation, not missing semantic context. Keep uncertainty and qualifiers.'}]
    inspected=[(['events',5,'attitude','target'],"the way that you're experiencing these thoughts",3398,3441,3394,3441)]
    inspected += [(['events',i,'attribution',owner,'binding_span'],'CATHERINE WIGGINTON-GREEN',4579,4605,4579,4604) for i in (8,9) for owner in ('transcript_voice','proposition_owner')]
    inspected += [(path,'CATHERINE WIGGINTON-GREEN:',4579,4606,4579,4605) for path in
        [['voice_bindings',4,'source_kind_evidence',0],['voice_bindings',4,'continuity_evidence',0]]]
    for path,text,start,end,fixed_start,fixed_end in inspected:
        span=original
        for key in path:span=span[key]
        if span!={'text':text,'start':start,'end':end} or source['transcript_window'].count(text)!=1 or source['transcript_window'][fixed_start:fixed_end]!=text:raise ValueError('inspected numeric span changed')
        for field,before,after in [('start',start,fixed_start),('end',end,fixed_end)]:
            if before!=after:changes.append({'path':path+[field],'before':before,'after':after,'reason':'Inspected unique exact source substring; numeric offset only, quoted text and identity unchanged.'})
    for i,binding in enumerate(original['voice_bindings']):
        anchor=binding['identity_anchor'];text=anchor['text'];start=anchor['start']
        if not text.endswith(':') or text[:-1]!=binding['voice_surface'] or source['transcript_window'][start:start+len(text)]!=text:raise ValueError('not inspected speaker-label anchor')
        fixed={'text':text[:-1],'start':start,'end':start+len(text)-1}
        changes.append({'path':['voice_bindings',i,'identity_anchor'],'before':anchor,'after':fixed,
            'reason':'Use the same exact name-only speaker label as the existing attribution binding, excluding label punctuation; no identity or corridor expansion.'})
    proposed,proof=propose(original,source=source['transcript_window'],window_id=WID,expected_original_sha256=digest(original),replacements=changes,output_validator=run.contract.validate)
    enc=tiktoken.get_encoding('o200k_base')
    ps=run.review.packets(proposed,source=source['transcript_window'],window_id=WID,token_count=lambda text:len(enc.encode(text)))
    r=next(p for p in ps if any(e['event_id']==EID for e in p['candidates']))
    r.pop('packet_sha256');r['candidates']=[e for e in r['candidates'] if e['event_id']==EID]
    refs={e['voice_binding_id'] for e in r['candidates']};r['voice_bindings']=[b for b in r['voice_bindings'] if b['voice_binding_id'] in refs]
    r['repair_provenance']=proof;r['system_sha256']=digest(SYSTEM);r['schema_sha256']=digest(run.review.schema(r));r['packet_sha256']=digest(r)
    tokens=len(enc.encode(SYSTEM+json.dumps(r,ensure_ascii=False)+json.dumps(run.review.schema(r))))+1500
    if tokens>12000:raise ValueError('full source exceeds review limit')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for name,v in [('proposal',proposed),('provenance',proof),(r['packet_sha256']+'.packet',r)]:immutable_json(OUT/f'{name}.json',v)
        immutable_json(OUT/'plan.json',{'packets':[r['packet_sha256']],'source_packet_sha256':p['packet_sha256'],'sidecar_sha256':digest(side),
            'original_records':11,'assigned_records':1,'input_tokens':tokens,'gold_accepted':False,'applied':False})
    return [r],proposed,proof


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps,_,_=prepare()
        print(json.dumps({'packets':1,'original_records':11,'applied':False}),flush=True)
        if args.execute:raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='hidden-brain-need-review-v1',system=SYSTEM,
            schema_for_packet=run.review.schema,validator=run.review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
