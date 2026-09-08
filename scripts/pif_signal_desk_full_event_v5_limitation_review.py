#!/usr/bin/env python3
"""Independent approval of one explicit candidate-to-quarantine proposal."""
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

WID='sdw_777d46db3fa4c592b71e';EID=WID+'_e8'
OUT=run.OUT/'explicit-limitation-review-v1'
SYSTEM=run.review.SYSTEM+"""\nThis packet reviews one explicit repair proposal from
the independently authored B response. Its before/after fields are supplied in
repair_provenance. Decide whether the actual source supports retaining this
vague statement as a quarantined research limitation needing wider context,
rather than a publishable substantive claim. Do not confuse quarantining with
deleting it: its ID, claim, source excerpt and denominator remain unchanged.
The proposal must not suppress a useful supported specific claim. If you disagree,
give a precise source-grounded correction or unresolved explanation. Review only
the candidates assigned in this packet, not every ID in the context record_index.
"""


def prepare(*, write=True):
    import tiktoken
    plan=json.loads((run.OUT/'plan.json').read_text())
    if plan['contract']!=run.author.receipt():raise ValueError('frozen author contract changed')
    source=json.loads((run.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text())
    p=run.author.packet(source,'B');d=run.OUT/'calls'/WID/'B';sha=p['packet_sha256']
    if json.loads((d/'packet.json').read_text())!=p:raise ValueError('B source/contract changed')
    sidecar=json.loads((d/f'{sha}.sidecar.json').read_text())
    if sidecar.get('state')!='completed' or sidecar.get('error_class'):raise ValueError('B provider call not completed')
    original=json.loads((d/f'{sha}.output.json').read_text())
    index=next(i for i,e in enumerate(original['events']) if e['event_id']==EID);e=original['events'][index]
    if len(original['events'])!=9 or e['evidence_role']['needs']!='wider_context' or e['evidence_text']!='on a federal level, you see affirmative action being impacted':
        raise ValueError('not the inspected underspecified statement')
    changes=[{'path':['events',index,'evidence_role','role'],'before':'substantive_claim','after':'research_limitation',
        'reason':'The statement does not specify what changed; preserve the explicit wider-context recovery need.'},
        {'path':['events',index,'publishability_state'],'before':'candidate','after':'quarantined',
        'reason':'Preserve the record and its denominator without publishing an underspecified claim pending source recovery.'}]
    proposed,provenance=propose(original,source=source['transcript_window'],window_id=WID,expected_original_sha256=digest(original),
        replacements=changes,output_validator=run.contract.validate)
    enc=tiktoken.get_encoding('o200k_base')
    all_packets=run.review.packets(proposed,source=source['transcript_window'],window_id=WID,token_count=lambda text:len(enc.encode(text)))
    r=next(r for r in all_packets if any(e['event_id']==EID for e in r['candidates']))
    r.pop('packet_sha256');r['candidates']=[e for e in r['candidates'] if e['event_id']==EID]
    refs={e['voice_binding_id'] for e in r['candidates']};r['voice_bindings']=[b for b in r['voice_bindings'] if b['voice_binding_id'] in refs]
    r['repair_provenance']=provenance;r['system_sha256']=digest(SYSTEM);r['schema_sha256']=digest(run.review.schema(r));r['packet_sha256']=digest(r)
    tokens=len(enc.encode(SYSTEM+json.dumps(r,ensure_ascii=False)+json.dumps(run.review.schema(r))))+1500
    if tokens>12000:raise ValueError('full-source repair exceeds review limit')
    if not write:return [r]
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    immutable_json(OUT/'proposal.json',proposed);immutable_json(OUT/'provenance.json',provenance)
    immutable_json(OUT/f"{r['packet_sha256']}.packet.json",r)
    immutable_json(OUT/'plan.json',{'packets':[r['packet_sha256']],'original_B_packet_sha256':sha,
        'original_sidecar_sha256':digest(sidecar),'system_sha256':digest(SYSTEM),'provenance_sha256':digest(provenance),
        'input_tokens':tokens,'gold_accepted':False,'applied':False})
    return [r]


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        print(json.dumps({'packets':len(ps),'assigned_records':1,'original_records':9,'gold_accepted':False}),flush=True)
        if args.execute:
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='full-event-v5-limitation-review-v1',system=SYSTEM,
                schema_for_packet=run.review.schema,validator=run.review.validate_review,
                packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
