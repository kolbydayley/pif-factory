#!/usr/bin/env python3
"""Read-only full-population inventory, including held predecessor outputs."""
import argparse
from collections import Counter
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run


def inventory(plan, *, require_complete=False):
    if len(plan['window_ids'])!=16 or len(set(plan['window_ids']))!=16:raise ValueError('full sixteen sources required')
    rows=[];complete={}
    for wid in plan['window_ids']:
        source=json.loads((run.previous.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text());outputs={};proofs={}
        for role in run.ROLES:
            row={'window_id':wid,'role':role,'state':'not_started'}
            if role=='C' and not {'A','B'}<=outputs.keys():row['state']='waiting_for_parents';rows.append(row);continue
            p=run.packet(source,role,outputs);sha=p['packet_sha256']
            old=run.previous.OUT/'calls'/wid/role
            imported=role!='C' and (old/'packet.json').exists()
            directory=old if imported else run.OUT/'calls'/wid/role
            result=directory/f'{sha}.result.json';side=directory/f'{sha}.sidecar.json'
            if result.exists():
                try:
                    outputs[role],proofs[role]=run.verified_call(directory,p,imported=imported)
                    value=outputs[role]['records'] if role=='C' else outputs[role]
                    row.update(state='verified_import' if imported else 'authored_not_accepted',records=len(value['events']))
                    if role=='C':row.update(input_dispositions=len(outputs[role]['input_dispositions']),additions=len(outputs[role]['additions']))
                except (ValueError,OSError) as exc:row.update(state='held',reason=str(exc))
            elif side.exists():
                s=json.loads(side.read_text());row.update(state='in_progress' if s.get('state')=='in_progress' else 'held',provider_state=s.get('state'))
                if s.get('error_class'):row['error_class']=s['error_class']
            elif (directory/'packet.json').exists():row['state']='awaiting_result_or_recovery'
            if imported:row['predecessor']=True
            rows.append(row)
        if len(outputs)==4:complete[wid]={'source':source,'outputs':outputs,'provenance':proofs}
    if require_complete and len(complete)!=16:raise ValueError(f'full role inventory incomplete: {len(complete)}/16 windows')
    return rows,complete


def summarize():
    plan=json.loads((run.OUT/'plan.json').read_text())
    if plan['lineage_contract']!=run.lineage.receipt() or plan['independent_role_contract']!=run.previous.author.receipt():raise ValueError('frozen contract changed')
    rows,complete=inventory(plan)
    return {'planned_windows':16,'planned_role_outputs':64,'complete_windows':len(complete),
            'states':dict(Counter(r['state'] for r in rows)),'calls':rows,'qualified':False,'gold_accepted':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--calls',action='store_true');args=parser.parse_args()
    result=summarize()
    if not args.calls:result.pop('calls')
    print(json.dumps(result,sort_keys=True))
