#!/usr/bin/env python3
"""Read-only full-population inventory, including held predecessor outputs."""
import argparse
from collections import Counter
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as run


def original_quality(directory, packet):
    """Count completed originals even when no valid result was ever produced."""
    raw_path = directory/f"{packet['packet_sha256']}.output.json"
    if not raw_path.exists():
        return {}
    result = {'original_raw_present': True}
    try:
        run.verify_provider(directory, packet)
        result['original_provider_verified'] = True
        raw = json.loads(raw_path.read_text())
        value = raw.get('records', {}) if packet['role'] == 'C' and isinstance(raw, dict) else raw
        if isinstance(value, dict) and isinstance(value.get('events'), list):
            result['original_record_count'] = len(value['events'])
        try:
            run.validate(raw, packet)
            result['original_first_pass_valid'] = True
        except (ValueError, TypeError, KeyError) as exc:
            result.update(original_first_pass_valid=False, original_validation_error=str(exc))
    except (ValueError, OSError, TypeError, KeyError) as exc:
        result['original_quality_unavailable_reason'] = str(exc)
    return result


def quality_summary(rows):
    verified = [r for r in rows if r['state'] in ('verified_import', 'authored_not_accepted')]
    raw = [r for r in rows if r.get('original_raw_present')]
    def counts(population):
        return dict(first_pass_valid=sum(r.get('original_first_pass_valid') is True for r in population),
            first_pass_invalid=sum(r.get('original_first_pass_valid') is False for r in population),
            explicitly_repaired=sum(r.get('repaired_output') is True for r in population))
    return {
        'verified_output_quality': dict(counts(verified),
            scope='Verified outputs selected for this diagnostic only; excludes superseded C attempts and pending/held outputs.'),
        'completed_raw_quality': dict(counts(raw), raw_outputs_present=len(raw),
            held_raw_outputs=sum(r['state'] == 'held' for r in raw),
            first_pass_unavailable=sum('original_first_pass_valid' not in r for r in raw),
            raw_records_observed=sum(r.get('original_record_count', 0) for r in raw),
            scope='All raw responses on the selected 64-role lineage, including held outputs. Structural validity only, not semantic approval. Superseded C attempts are separate history; unstarted roles are not successful samples.')}


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
                    raw=json.loads((directory/f'{sha}.output.json').read_text())
                    row['repaired_output']=raw!=outputs[role]
                    if role=='C':row.update(input_dispositions=len(outputs[role]['input_dispositions']),additions=len(outputs[role]['additions']))
                except (ValueError,OSError) as exc:row.update(state='held',reason=str(exc))
            elif side.exists():
                s=json.loads(side.read_text());row.update(state='in_progress' if s.get('state')=='in_progress' else 'held',provider_state=s.get('state'))
                if s.get('error_class'):row['error_class']=s['error_class']
            elif (directory/'packet.json').exists():row['state']='awaiting_result_or_recovery'
            row.update(original_quality(directory, p))
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
            **quality_summary(rows),
            'states':dict(Counter(r['state'] for r in rows)),'calls':rows,'qualified':False,'gold_accepted':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--calls',action='store_true');args=parser.parse_args()
    result=summarize()
    if not args.calls:result.pop('calls')
    print(json.dumps(result,sort_keys=True))
