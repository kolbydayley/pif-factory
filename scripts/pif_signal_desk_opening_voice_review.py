#!/usr/bin/env python3
"""Full-source diagnosis of readable claims with unresolved opening identities."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_full_event_v4_review import packets
from research_factory.signal_desk_rubric_reference_packets import digest

OUT=run.OUT/'opening-voice-source-diagnosis-v1'
CASES={
 'sdw_8bc85d983495f3780cb2':('a41014004e553cb84aa28e8c043c96bd8ca7c9e514b5afadeb8b8d4afc719cee','20292551810b36fb607612139b10571532dbd542c5b533be4075a4abe216b3e4',12),
 'sdw_4acaaec65ea71658126b':('d0f4f279d15b66c882a262b078f8b8bc39d355978c9d485146d8a1d913f02516','77695c587211dd21da57e3f1c3f824277fef37920c8bbe6dca9e5dfb04a84dc9',14)}
SYSTEM=run.previous.review.SYSTEM+"""\nHELD-INPUT SOURCE DIAGNOSIS:
The original records are unaccepted and structurally held. Review every assigned
record against the complete supplied window, including attribution, useful content,
source/identity recovery, atomicity and contamination. Opening paragraphs are
unlabeled. Never infer their voice from a later named turn or outside knowledge.
Distinguish: (1) intelligible content with unresolved speaker, (2) unintelligible
content requiring source recovery, (3) understandable reported figures requiring
external verification. The inherited evidence-role validator permits a non-none
needs field only on research_limitation, and research_limitation must be quarantined.
That validator restriction is not evidence that a readable claim is useless.
Recommend the source-supported representation under the current contract, or state
clearly when that contract cannot express the necessary distinction. Do not guess
names, recommend dropping records to pass, or hide unavailable attribution. A
null-voice record cannot be published as a named person's own words. Do not equate
source-supported diagnosis with acceptance or publication authorization. Candidate
offsets may be wrong: verify the source text independently and state exact fixes.
Preserve explicit sponsor exclusions and separate host questions/opinions from
guest assertions. All26 original records remain in the diagnostic population.
"""


def prepare(*,write=True):
    import tiktoken
    plan=json.loads((run.OUT/'plan.json').read_text());enc=tiktoken.get_encoding('o200k_base');ps=[];sources=[]
    for wid,(sha,raw_sha,count) in CASES.items():
        source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
        p=run.packet(source,'A',{});d=run.OUT/'calls'/wid/'A';side=run.verify_provider(d,p)
        raw=json.loads((d/f'{sha}.output.json').read_text())
        if p['packet_sha256']!=sha or digest(raw)!=raw_sha or len(raw['events'])!=count:raise ValueError('not the inspected source response')
        def inspected(value,*,source,window_id):
            if window_id!=wid or source!=p['transcript_window'] or digest(value)!=raw_sha:raise ValueError('diagnostic input changed')
        ps.extend(packets(raw,source=p['transcript_window'],window_id=wid,token_count=lambda s:len(enc.encode(s)),
            system=SYSTEM,schema_for_packet=run.previous.review.schema,output_validator=inspected))
        sources.append({'window_id':wid,'packet_sha256':sha,'raw_sha256':raw_sha,'records':count,'sidecar_sha256':digest(side)})
    if sum(len(p['candidates']) for p in ps)!=26:raise ValueError('diagnostic population changed')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for p in ps:run.immutable_json(OUT/f"{p['packet_sha256']}.packet.json",p)
        run.immutable_json(OUT/'plan.json',{'packets':[p['packet_sha256'] for p in ps],'sources':sources,'records':26,
            'diagnosis_only':True,'applied':False,'qualified':False,'gold_accepted':False})
    return ps


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare();print(json.dumps({'packets':len(ps),'records':26,'diagnosis_only':True}),flush=True)
        if args.execute:
            review=run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='opening-voice-source-diagnosis-v1',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
