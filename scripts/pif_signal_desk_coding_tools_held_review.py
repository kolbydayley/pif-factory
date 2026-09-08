#!/usr/bin/env python3
"""Diagnose one explicitly held raw response; does not qualify invalid inputs."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory import signal_desk_full_event_v4_review as packing
from research_factory.signal_desk_rubric_reference_packets import digest

WID='sdw_4117ea30ae4ec3f116ef'
RAW='fccf910e61e76ba4ccc6fe49ec1650e12dce5641866502322ce8efeccdb60021'
SOURCE='71ca0f8e13f9e570826e9f5a3ec81f1306a8ec1789b6075121d31ad85bbdca1c'
PACKET='818ced84576324345a2661dbff5108d4fce97a8d5f7cd70963e778d2758f89f5'
OUT=run.OUT/'coding-tools-held-source-review-v1'
SYSTEM=run.previous.review.SYSTEM+"""\nHELD-INPUT DIAGNOSIS, NOT FINAL ACCEPTANCE:
These are original unaccepted records that failed validation. Some supplied nested
offsets are wrong. Inspect the full transcript directly; candidate offsets and
claims are not authoritative. Return exact source quotes, independently assessing
every assigned record's meaning, attribution, atomicity, and recovery needs.
Distinguish verification uncertainty from missing semantic source context. Do not
repair damaged ASR words or speaker labels using external knowledge. A quarantine
disposition alone does not fix a distorted claim. Specify exact semantic changes
or what remains unresolvable. Numeric mistakes must remain visible, but do not let
them hide semantic errors. Examine possible added temporal or causal relationships,
overstated strategic judgments, and compound claims. Do not merge Shawn and Swyx
using outside identity knowledge. This review diagnoses all 15 original records;
none are removed or promoted by a successful reviewer response. Application of any
correction requires a separate source-bound proposal and verification.
"""


def inspected_invalid_input(value,*,source,window_id):
    if window_id!=WID or digest(source)!=SOURCE or digest(value)!=RAW or len(value['events'])!=15:
        raise ValueError('not the inspected held response')
    try:run.previous.contract.validate(value,source=source,window_id=window_id)
    except ValueError as exc:
        if str(exc)!='inexact span':raise ValueError('held failure changed') from exc
        return value
    raise ValueError('held input unexpectedly validates')


def prepare(*,write=True):
    import tiktoken
    plan=json.loads((run.OUT/'plan.json').read_text())
    source=json.loads((run.previous.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text())
    p=run.packet(source,'A',{});d=run.OUT/'calls'/WID/'A';side=run.verify_provider(d,p)
    if p['packet_sha256']!=PACKET:raise ValueError('original request changed')
    raw=json.loads((d/f'{PACKET}.output.json').read_text())
    enc=tiktoken.get_encoding('o200k_base')
    ps=packing.packets(raw,source=p['transcript_window'],window_id=WID,token_count=lambda s:len(enc.encode(s)),
        system=SYSTEM,schema_for_packet=run.previous.review.schema,output_validator=inspected_invalid_input)
    if [e['event_id'] for q in ps for e in q['candidates']]!=[e['event_id'] for e in raw['events']]:raise ValueError('review population changed')
    if write:
        OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
        for q in ps:run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json",q)
        run.immutable_json(OUT/'plan.json',{'packets':[q['packet_sha256'] for q in ps],'source_packet_sha256':PACKET,
            'original_output_sha256':RAW,'original_validation_error':'inexact span','sidecar_sha256':digest(side),
            'original_records':15,'diagnosis_only':True,'applied':False,'qualified':False,'gold_accepted':False})
    return ps


def status():
    ps=prepare(write=False);rows=[]
    saved=json.loads((OUT/'plan.json').read_text())
    if saved['packets']!=[p['packet_sha256'] for p in ps] or saved['original_output_sha256']!=RAW:
        raise ValueError('diagnostic plan changed')
    h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    for p in ps:
        sha=p['packet_sha256'];row={'packet_sha256':sha,'assigned_records':len(p['candidates'])}
        if not (OUT/f'{sha}.sidecar.json').exists():
            row['state']='not_started';rows.append(row);continue
        try:
            side=json.loads((OUT/f'{sha}.sidecar.json').read_text())
            if side.get('state')!='completed':
                row.update(state=side.get('state'),error_class=side.get('error_class'));rows.append(row);continue
            if side.get('error_class') or side.get('model')!='gpt-5.5' or side.get('effort')!='high':raise ValueError('review provider mismatch')
            if side.get('base_instructions_sha256')!=h(SYSTEM) or side.get('prompt_sha256')!=h(json.dumps(p,ensure_ascii=False)):
                raise ValueError('review request mismatch')
            if json.loads((OUT/f'{sha}.packet.json').read_text())!=p:raise ValueError('saved review packet changed')
            value=json.loads((OUT/f'{sha}.review.json').read_text())
            if value!=json.loads((OUT/f'{sha}.output.json').read_text()):raise ValueError('review output changed')
            run.previous.review.validate_review(value,p)
            row.update(state='verified_diagnosis',decisions=value['decisions'],coverage_notes=value['coverage_notes'])
        except (OSError,ValueError) as exc:row.update(state='unverified',reason=str(exc))
        rows.append(row)
    return {'packets':rows,'all_reviews_verified':all(r['state']=='verified_diagnosis' for r in rows),
            'original_records':15,'diagnosis_only':True,'qualified':False,'gold_accepted':False}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');parser.add_argument('--status',action='store_true');args=parser.parse_args()
    if args.status:
        print(json.dumps(status(),ensure_ascii=False),flush=True);return
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        print(json.dumps({'packets':len(ps),'original_records':15,'diagnosis_only':True}),flush=True)
        if args.execute:
            review=run.previous.review
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='coding-tools-held-source-review-v1',system=SYSTEM,
                schema_for_packet=review.schema,validator=review.validate_review,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))


if __name__=='__main__':main()
