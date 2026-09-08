#!/usr/bin/env python3
"""Review an explicit full-population repair; never apply it automatically."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_coding_tools_held_review as diagnosis
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_coding_tools_repair import prepare as proposal
from research_factory.signal_desk_full_event_v4_review import packets
from research_factory.signal_desk_rubric_reference_packets import digest

OUT=diagnosis.run.OUT/'coding-tools-explicit-repair-review-v1'
SYSTEM=diagnosis.run.previous.review.SYSTEM+"""\nEXPLICIT REPAIR REVIEW:
Review every assigned proposed record against the complete source, independently.
The proposal preserves all15 original IDs and voice corridors. It fixes exact
offsets; preserves the ASR ID form-factor wording; makes the garbled forecast's
attitude indeterminate; removes unsupported relationship chronology; sets recovery
needs to none on intelligible but uncertain e01/e02/e12/e14/e15; quarantines e09/e10
as research limitations while preserving their unresolved source/audio needs.
For e05-e08, attribution binding spans now use the existing Swyx voice-binding
anchor at1310 instead of repeated same-label anchor1582, without expanding the
corridor or inferring a new identity. Examine that choice explicitly. Check whether
quarantine over-excludes intelligible content, or needs=none hides real source
limitations. Neither previous reviewer support nor schema validity mandates your
approval. State exact corrections or unresolved needs when warranted. No accepted
gold status or automatic application results from this review.
"""


def prepare(*,write=True):
    import tiktoken
    p,fixed,proof=proposal();enc=tiktoken.get_encoding('o200k_base');review=diagnosis.run.previous.review
    ps=packets(fixed,source=p['transcript_window'],window_id=diagnosis.WID,
        token_count=lambda s:len(enc.encode(s))+600,system=SYSTEM,schema_for_packet=review.schema,
        output_validator=diagnosis.run.previous.contract.validate)
    for q in ps:
        q.pop('packet_sha256');q['proposal_sha256']=proof['proposed_sha256'];q['repair_proof_sha256']=digest(proof)
        q['packet_sha256']=digest(q)
        if len(enc.encode(SYSTEM+json.dumps(q,ensure_ascii=False)+json.dumps(review.schema(q))))+1500>12000:raise ValueError('review budget exceeded')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for name,v in [('proposal',fixed),('provenance',proof)]:diagnosis.run.immutable_json(OUT/f'{name}.json',v)
        for q in ps:diagnosis.run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json",q)
        diagnosis.run.immutable_json(OUT/'plan.json',{'packets':[q['packet_sha256'] for q in ps],
            'original_records':15,'proposal_sha256':digest(fixed),'provenance_sha256':digest(proof),'applied':False,'gold_accepted':False})
    return ps,fixed,proof


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (diagnosis.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps,_,_=prepare()
        print(json.dumps({'packets':len(ps),'records':15,'applied':False}),flush=True)
        if args.execute:
            review=diagnosis.run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='coding-tools-explicit-repair-review-v1',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
