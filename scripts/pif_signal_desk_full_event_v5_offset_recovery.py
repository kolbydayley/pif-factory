#!/usr/bin/env python3
"""Explicit inspected offset recovery after the author worker has drained."""
import argparse
import fcntl
import json
import sqlite3
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts import pif_signal_desk_full_event_v5_run as run
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_v5_offset_recovery import WID,recover
from research_factory.signal_desk_full_event_v5_registered_offsets import recover as registered_recover
from research_factory.signal_desk_rebuild_dispatch import resurrect_task,acquire_lease,complete_attempt


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true')
    parser.add_argument('--case',choices=['changelog-a','changelog-b','marketplace-c'],default='changelog-a');args=parser.parse_args()
    wid='sdw_777d46db3fa4c592b71e' if args.case=='marketplace-c' else WID
    role={'changelog-a':'A','changelog-b':'B','marketplace-c':'C'}[args.case]
    with (run.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        plan=run.prepare();source=json.loads((run.BASE/f"{plan['source_packets'][wid]}.packet.json").read_text())
        authors={}
        if role=='C':
            from research_factory.signal_desk_full_event_v5_offset_recovery import load_call
            for r in ['A','B']:
                parent=run.author.packet(source,r);authors[r],_=load_call(run.OUT/'calls'/wid/r,parent)
        p=run.author.packet(source,role,author_a=authors.get('A'),author_b=authors.get('B'));d=run.OUT/'calls'/wid/role;sha=p['packet_sha256']
        if json.loads((d/'packet.json').read_text())!=p:raise ValueError('source/contract changed')
        sidecar=json.loads((d/f'{sha}.sidecar.json').read_text())
        if sidecar.get('state')!='completed' or sidecar.get('error_class'):raise ValueError('provider result not complete')
        original=json.loads((d/f'{sha}.output.json').read_text())
        fixed,receipt=(recover(original,source=source['transcript_window'],window_id=wid) if args.case=='changelog-a' else registered_recover(original,p))
        print(json.dumps(receipt),flush=True)
        if not args.execute:return
        db=sqlite3.connect(d/'dispatch.sqlite');db.row_factory=sqlite3.Row
        try:
            key='full-event-v5-context-qualification:'+sha
            row=db.execute('SELECT a.* FROM signal_desk_rebuild_tasks t JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id WHERE t.task_key=?',(key,)).fetchone()
            expected_error='inexact evidence' if args.case=='changelog-b' else 'span not exact source'
            if row is None or row['status']!='terminal_failed' or row['semantic_failure_detail']!=expected_error:
                raise ValueError('not the inspected terminal semantic failure')
            immutable_json(d/'offset-recovery.json',receipt)
            immutable_json(d/f'{sha}.result.json',fixed)
            resurrect_task(db,task_key=key,resurrected_by='01a04040-77ee-77f2-9d26-a5ca1ae56986',
                reason='Explicit source-inspected nested-span offset projection; original text, fields, failure, and first-pass counts preserved; no gold approval')
            lease=acquire_lease(db,lease_owner='explicit-v5-offset-recovery',lease_seconds=1800,task_key_prefix=key)
            complete_attempt(db,attempt_id=lease['current_attempt_id'],lease_owner=lease['lease_owner'],lease_generation=lease['lease_generation'],output=fixed)
        finally:db.close()


if __name__=='__main__':main()
