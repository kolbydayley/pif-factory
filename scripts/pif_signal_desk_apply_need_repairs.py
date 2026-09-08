#!/usr/bin/env python3
"""Apply only independently supported, exactly reconstructed repair projections."""
import argparse
import fcntl
import json
import sqlite3
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from research_factory.signal_desk_reviewed_need_recovery import case_module,recover
from research_factory.signal_desk_rebuild_dispatch import resurrect_task,acquire_lease,complete_attempt
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from scripts import pif_signal_desk_lineage_qualification as lane


def apply_verified(directory,p,fixed,proof,prefix,*,execute=False,expected_failure='source limitation requires recovery need',receipt_name='source-need-repair.json'):
    sha=p['packet_sha256'];key=prefix+':'+sha
    db=sqlite3.connect(directory/'dispatch.sqlite');db.row_factory=sqlite3.Row
    try:
        row=db.execute('SELECT a.* FROM signal_desk_rebuild_tasks t JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id WHERE t.task_key=?',(key,)).fetchone()
        if row is None:raise ValueError('original task lineage missing')
        if row['status']=='succeeded':
            if json.loads((directory/f'{sha}.result.json').read_text())!=fixed or json.loads((directory/receipt_name).read_text())!=proof:raise ValueError('completed repair differs')
            return {'already_applied':True,'gold_accepted':False}
        if row['status']!='terminal_failed' or row['semantic_failure_detail']!=expected_failure:raise ValueError('not the inspected terminal failure')
        if execute:
            immutable_json(directory/receipt_name,proof);immutable_json(directory/f'{sha}.result.json',fixed)
            resurrect_task(db,task_key=key,resurrected_by='01a04040-77ee-77f2-9d26-a5ca1ae56986',
                reason='Explicit source-verified repair with saved provenance; semantic need corrections independently approved by GPT-5.5, offsets preserve quoted text. Original raw output and failure retained. Not accepted gold.')
            lease=acquire_lease(db,lease_owner='reviewed-source-need-repair',lease_seconds=1800,task_key_prefix=key)
            complete_attempt(db,attempt_id=lease['current_attempt_id'],lease_owner=lease['lease_owner'],lease_generation=lease['lease_generation'],output=fixed)
        return {'applied':execute,'gold_accepted':False,'proof':proof}
    finally:db.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--case',choices=['tsmc','hidden-brain','hidden-brain-b','hidden-brain-c','hidden-brain-audit','ai-governance-a','ai-governance-b','marketplace','changelog-audit-offsets','coding-tools-v3'],required=True);parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (lane.previous.parent.OUT/'runner.lock').open('a') as a,(lane.previous.OUT/'runner.lock').open('a') as b:
        for lock in (a,b):fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.case=='tsmc':
            from scripts.pif_signal_desk_tsmc_repair_review import proposal
            from research_factory.signal_desk_tsmc_recovery import recover as correction
            _,_,p=proposal();directory=lane.OUT/'calls'/p['window_id']/'A'
            raw=json.loads((directory/f"{p['packet_sha256']}.output.json").read_text());fixed,proof=correction(raw,p)
            print(json.dumps(apply_verified(directory,p,fixed,proof,'full-event-lineage-qualification-v1',execute=args.execute,
                expected_failure='inexact span',receipt_name='tsmc-repair.json')),flush=True);return
        if args.case in {'hidden-brain-audit','ai-governance-a','ai-governance-b'}:
            from research_factory.signal_desk_september8_offsets import recover as offsets,CASES
            spec=CASES[args.case];directory=lane.OUT/'calls'/spec['window']/spec['role']
            plan=json.loads((lane.OUT/'plan.json').read_text());source=json.loads((lane.previous.BASE/f"{plan['source_packets'][spec['window']]}.packet.json").read_text())
            p=lane.packet(source,spec['role'],{});lane.verify_provider(directory,p)
            raw=json.loads((directory/f"{p['packet_sha256']}.output.json").read_text());fixed,proof=offsets(args.case,raw,p)
            print(json.dumps(apply_verified(directory,p,fixed,proof,'full-event-lineage-qualification-v1',execute=args.execute,
                expected_failure=spec.get('failure','span not exact source'),receipt_name='september8-offset-repair.json')),flush=True);return
        if args.case=='hidden-brain-c':
            from scripts.pif_signal_desk_hidden_brain_c_repair_review import WID
            from research_factory.signal_desk_hidden_brain_c_recovery import recover as correction
            directory=lane.OUT/'calls'/WID/'C';p=json.loads((directory/'packet.json').read_text())
            raw=json.loads((directory/f"{p['packet_sha256']}.output.json").read_text());fixed,proof=correction(raw,p)
            print(json.dumps(apply_verified(directory,p,fixed,proof,'full-event-lineage-qualification-v1',execute=args.execute,
                expected_failure='inexact record evidence',receipt_name='hidden-brain-c-repair.json')),flush=True);return
        if args.case=='coding-tools-v3':
            from scripts.pif_signal_desk_coding_tools_held_review import WID
            from research_factory.signal_desk_coding_tools_final_recovery import recover as coding
            directory=lane.OUT/'calls'/WID/'A';p=json.loads((directory/'packet.json').read_text())
            raw=json.loads((directory/f"{p['packet_sha256']}.output.json").read_text());fixed,proof=coding(raw,p)
            print(json.dumps(apply_verified(directory,p,fixed,proof,'full-event-lineage-qualification-v1',execute=args.execute,
                expected_failure='inexact span',receipt_name='coding-tools-repair.json')),flush=True);return
        if args.case=='changelog-audit-offsets':
            from research_factory.signal_desk_lineage_audit_offsets import recover as offsets,WID
            plan=json.loads((lane.OUT/'plan.json').read_text());source=json.loads((lane.previous.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text())
            p=lane.packet(source,'AUDIT',{});directory=lane.OUT/'calls'/WID/'AUDIT';lane.verify_provider(directory,p)
            original=json.loads((directory/f"{p['packet_sha256']}.output.json").read_text());fixed,proof=offsets(original,p)
            print(json.dumps(apply_verified(directory,p,fixed,proof,'full-event-lineage-qualification-v1',execute=args.execute,
                expected_failure='inexact evidence',receipt_name='inspected-offset-repair.json')),flush=True);return
        module=case_module(args.case);role={'hidden-brain':'A','hidden-brain-b':'B','marketplace':'C'}[args.case]
        directory=module.run.OUT/'calls'/module.WID/role;p=json.loads((directory/'packet.json').read_text())
        original=json.loads((directory/f"{p['packet_sha256']}.output.json").read_text());fixed,proof=recover(args.case,original,p)
        prefix='full-event-v5-context-qualification' if args.case=='hidden-brain' else 'full-event-lineage-qualification-v1'
        print(json.dumps(apply_verified(directory,p,fixed,proof,prefix,execute=args.execute)),flush=True)


if __name__=='__main__':main()
