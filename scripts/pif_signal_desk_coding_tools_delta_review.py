#!/usr/bin/env python3
"""Four-record repair delta; eleven unchanged approvals retain their proof."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_coding_tools_repair_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_coding_tools_recovery import inspect_reviews
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest

OUT=parent.diagnosis.run.OUT/'coding-tools-explicit-repair-review-v2'
INDICES=(9,11,13,14)
SYSTEM=parent.SYSTEM+"""\nV2 DELTA: Only four assigned records changed. The original
full15 population stays in record_index. e10's main evidence now includes the
preceding thesis-died sentence within the same voice corridor. e12 and e15 remove
currency signs not specified by this transcript. e14's own forecast is scoped as
attributed_view, while its uncertain premises remain intact. Review these changes
and the whole assigned records independently; do not repeat other records' reviews.
Do not infer currencies from industry knowledge. No labels are applied by this run.
"""


def prepare(*,write=True):
    import tiktoken
    d=parent.diagnosis.run.OUT/'calls'/parent.diagnosis.WID/'A'
    packet=json.loads((d/'packet.json').read_text());raw=json.loads((d/f'{parent.diagnosis.PACKET}.output.json').read_text())
    baseline,prior_proof,decisions=inspect_reviews(raw,packet)
    if digest(baseline)!='ca82602fcfc9bd3deeb5019d9682f0a75ea6845806e633f4193b4f93552aec87':raise ValueError('uninspected baseline')
    source=packet['transcript_window'];changes=[]
    def change(path,after,reason):
        v=baseline
        for k in path:v=v[k]
        changes.append({'path':path,'before':v,'after':after,'reason':reason})
    if not source[3018:3096].startswith('My thesis has, has gone has died basically'):raise ValueError('source context changed')
    change(['events',9,'evidence_start'],3018,'Include the exact preceding thesis-died sentence explicitly required by independent review.')
    change(['events',9,'evidence_text'],source[3018:baseline['events'][9]['evidence_end']],
           'Ground every claim component in its own evidence, within the unchanged Shawn corridor.')
    for i in (11,14):
        change(['events',i,'claim_text'],baseline['events'][i]['claim_text'].replace('$',''),
               'Source supplies numerical amounts but no currency; remove external currency assumption consistently.')
    change(['events',13,'evidence_role','scope'],'attributed_view',
           'The durability forecast is explicitly the speaker own view, not an attributed third-party forecast.')
    fixed,proof=propose(baseline,source=source,window_id=parent.diagnosis.WID,expected_original_sha256=digest(baseline),
                        replacements=changes,output_validator=parent.diagnosis.run.previous.contract.validate)
    changed={baseline['events'][i]['event_id'] for i in INDICES};by_id={d['event_id']:d for d in decisions}
    for before,after in zip(baseline['events'],fixed['events']):
        if before['event_id'] not in changed and (before!=after or by_id[before['event_id']]['verdict']!='supported'):
            raise ValueError('unapproved unchanged record')
    proof['prior_reviews_proof_sha256']=digest(prior_proof);proof['unchanged_supported_records']=11
    enc=tiktoken.get_encoding('o200k_base');review=parent.diagnosis.run.previous.review
    full=parent.packets(fixed,source=source,window_id=parent.diagnosis.WID,token_count=lambda s:len(enc.encode(s))+600,
        system=SYSTEM,schema_for_packet=review.schema,output_validator=parent.diagnosis.run.previous.contract.validate)
    ps=[]
    for p in full:
        candidates=[e for e in p['candidates'] if e['event_id'] in changed]
        if not candidates:continue
        p.pop('packet_sha256');p['candidates']=candidates
        refs={e['voice_binding_id'] for e in candidates};p['voice_bindings']=[b for b in p['voice_bindings'] if b['voice_binding_id'] in refs]
        p['proposal_sha256']=digest(fixed);p['repair_proof_sha256']=digest(proof);p['schema_sha256']=digest(review.schema(p));p['packet_sha256']=digest(p)
        if len(enc.encode(SYSTEM+json.dumps(p,ensure_ascii=False)+json.dumps(review.schema(p))))+1500>12000:raise ValueError('review packet exceeds budget')
        ps.append(p)
    if {e['event_id'] for p in ps for e in p['candidates']}!=changed:raise ValueError('changed population lost')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for name,v in [('proposal',fixed),('provenance',proof)]:parent.diagnosis.run.immutable_json(OUT/f'{name}.json',v)
        for p in ps:parent.diagnosis.run.immutable_json(OUT/f"{p['packet_sha256']}.packet.json",p)
        parent.diagnosis.run.immutable_json(OUT/'plan.json',{'packets':[p['packet_sha256'] for p in ps],'records':15,'reviewed_delta_records':4,
            'unchanged_supported_records':11,'proposal_sha256':digest(fixed),'provenance_sha256':digest(proof),'applied':False,'gold_accepted':False})
    return ps,fixed,proof


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (parent.diagnosis.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps,_,_=prepare();print(json.dumps({'packets':len(ps),'delta_records':4,'records':15}),flush=True)
        if args.execute:
            review=parent.diagnosis.run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='coding-tools-explicit-repair-review-v2',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
