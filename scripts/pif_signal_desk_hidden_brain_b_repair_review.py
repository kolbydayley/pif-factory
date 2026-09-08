#!/usr/bin/env python3
"""Independent review of one source-bound B correction, never automatic approval."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest

WID='sdw_99a1771e94fa2b923f9e';EID='evt_002';ROLE='B'
OUT=run.OUT/'hidden-brain-b-need-review-v1'
SYSTEM=run.previous.review.SYSTEM+"""\nReview this explicit one-field repair against
the complete source. The proposed change is evidence_role.needs: wider_context
to none for Summerville's hedged prevalence estimate. The original nine records,
claim text, uncertainty, attribution, and rationale are unchanged. Missing study
methods may limit verification without making the speaker's claim unintelligible.
Decide independently whether this distinction applies here. Do not assume that
schema validity establishes truth, usefulness, publication readiness, or approval.
Review only the assigned candidate; reject or request correction if warranted.
"""


def prepare(*,write=True):
    import tiktoken
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text())
    p=run.packet(source,ROLE,{})
    d=run.OUT/'calls'/WID/ROLE;side=run.verify_provider(d,p);sha=p['packet_sha256']
    original=json.loads((d/f'{sha}.output.json').read_text())
    if sha!='bc7b247f0cdb2f2a988cfdc1ce103cb50c0279998ec2621fc9142e7de69155fb' or digest(original)!='956c2a59db6858ebbfceb689500e40be165c5102d752d1a8fbb593d16d65d5ab':
        raise ValueError('not inspected source response')
    index=next(i for i,e in enumerate(original['events']) if e['event_id']==EID)
    if len(original['events'])!=9 or original['events'][index]['publishability_state']!='uncertain':raise ValueError('population or uncertainty changed')
    changes=[{'path':['events',index,'evidence_role','needs'],'before':'wider_context','after':'none',
              'reason':'The attributed and hedged estimate is intelligible; unspecified study methods limit verification, not source interpretation. Preserve uncertainty.'}]
    fixed,proof=propose(original,source=source['transcript_window'],window_id=WID,expected_original_sha256=digest(original),
                        replacements=changes,output_validator=run.previous.contract.validate)
    enc=tiktoken.get_encoding('o200k_base');review=run.previous.review
    packets=review.packets(fixed,source=source['transcript_window'],window_id=WID,token_count=lambda t:len(enc.encode(t)))
    r=next(p for p in packets if any(e['event_id']==EID for e in p['candidates']))
    r.pop('packet_sha256');r['candidates']=[e for e in r['candidates'] if e['event_id']==EID]
    refs={e['voice_binding_id'] for e in r['candidates']};r['voice_bindings']=[b for b in r['voice_bindings'] if b['voice_binding_id'] in refs]
    r['repair_provenance']=proof;r['system_sha256']=digest(SYSTEM);r['schema_sha256']=digest(review.schema(r));r['packet_sha256']=digest(r)
    tokens=len(enc.encode(SYSTEM+json.dumps(r,ensure_ascii=False)+json.dumps(review.schema(r))))+1500
    if tokens>12000:raise ValueError('full source exceeds review limit')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for name,v in [('proposal',fixed),('provenance',proof),(r['packet_sha256']+'.packet',r)]:run.immutable_json(OUT/f'{name}.json',v)
        run.immutable_json(OUT/'plan.json',{'packets':[r['packet_sha256']],'source_packet_sha256':sha,'sidecar_sha256':digest(side),
            'original_records':9,'assigned_records':1,'input_tokens':tokens,'gold_accepted':False,'applied':False})
    return [r],fixed,proof


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps,_,_=prepare()
        print(json.dumps({'packets':1,'original_records':9,'applied':False}),flush=True)
        if args.execute:
            review=run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='hidden-brain-b-need-review-v1',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
