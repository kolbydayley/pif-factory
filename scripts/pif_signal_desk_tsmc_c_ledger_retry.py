"""Bounded fresh review of four items from one invalid completed ledger response."""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_tsmc_c_delta_review as previous
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_rubric_reference_packets import digest
OUT=previous.OUT.parent/'tsmc-c-final-ledger-review-v3'
FAILED='9d1f9a1d7366a5f17feefac6c81969b719187eed85cedef7554993737072ec2e'
IDS={'input-0025','input-0026','addition-0000','addition-0001'}
SYSTEM=previous.LEDGER_SYSTEM+'''\nSource quotations must not prepend an inferred speaker label. A person's
attribution may be supported by the labeled corridor, but the source_quotes string
must contain only characters that occur contiguously in the transcript. Independently
decide every assigned mapping and addition; do not assume a previous verdict.
'''


def prepare(*,write=True):
    selected,_=previous.prepare(write=False)
    old=next(p for p in selected['ledger'] if p['packet_sha256']==FAILED)
    d=previous.OUT/'ledger'
    side=json.loads((d/f'{FAILED}.sidecar.json').read_text())
    raw=json.loads((d/f'{FAILED}.output.json').read_text())
    h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    if (side.get('state')!='completed' or side.get('error_class') or side.get('model')!='gpt-5.5'
        or side.get('effort')!='high' or side.get('base_instructions_sha256')!=h(previous.LEDGER_SYSTEM)
        or side.get('prompt_sha256')!=h(json.dumps(old,ensure_ascii=False))):
        raise ValueError('failed review provenance changed')
    if json.loads((d/f'{FAILED}.packet.json').read_text())!=old:raise ValueError('failed packet changed')
    invalid=[(r['decision_id'],q) for r in raw['decisions'] for q in r['source_quotes'] if q not in old['transcript_window']]
    if invalid!=[('addition-0000','Morris: NVIDIA was just one of the customers.')]:
        raise ValueError('unexpected invalid quote population')
    if {c['decision_id'] for c in old['candidates']}!=IDS:raise ValueError('retry population changed')
    q=dict(old);q.pop('packet_sha256');q['system_sha256']=digest(SYSTEM)
    q['packet_sha256']=digest(q)
    proof=dict(failed_packet_sha256=FAILED,failed_sidecar_sha256=digest(side),failed_output_sha256=digest(raw),
               invalid_quote_count=1,original_preserved=True,gold_accepted=False)
    if write:
        previous.parent.run.immutable_json(OUT/'provenance.json',proof)
        previous.parent.run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json",q)
        previous.parent.run.immutable_json(OUT/'plan.json',dict(packets=[q['packet_sha256']],records=4,qualified=False,gold_accepted=False))
    return [q]


def verified_review():
    p=prepare(write=False)[0]
    return previous.verify(OUT,p,system=SYSTEM,validator=previous.parent.ledger.validate)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (previous.parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);ps=prepare()
        if args.execute:
            provider=previous.parent.ledger
            raise SystemExit(asyncio.run(execute(ps,output_root=OUT,task_prefix='tsmc-c-final-ledger-v3',system=SYSTEM,
                schema_for_packet=provider.schema,validator=provider.validate,packet_id=lambda p:p['packet_sha256'],verdict_rows=lambda v:v['decisions'])))
