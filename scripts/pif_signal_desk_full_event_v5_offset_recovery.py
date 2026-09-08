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
from research_factory.signal_desk_rebuild_dispatch import resurrect_task,acquire_lease,complete_attempt


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        plan=run.prepare();source=json.loads((run.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text())
        p=run.author.packet(source,'A');d=run.OUT/'calls'/WID/'A';sha=p['packet_sha256']
        if json.loads((d/'packet.json').read_text())!=p:raise ValueError('source/contract changed')
        sidecar=json.loads((d/f'{sha}.sidecar.json').read_text())
        if sidecar.get('state')!='completed' or sidecar.get('error_class'):raise ValueError('provider result not complete')
        original=json.loads((d/f'{sha}.output.json').read_text())
        fixed,receipt=recover(original,source=source['transcript_window'],window_id=WID)
        print(json.dumps(receipt),flush=True)
        if not args.execute:return
        db=sqlite3.connect(d/'dispatch.sqlite');db.row_factory=sqlite3.Row
        try:
            key='full-event-v5-context-qualification:'+sha
            row=db.execute('SELECT a.* FROM signal_desk_rebuild_tasks t JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id WHERE t.task_key=?',(key,)).fetchone()
            if row is None or row['status']!='terminal_failed' or row['semantic_failure_detail']!='span not exact source':
                raise ValueError('not the inspected terminal semantic failure')
            immutable_json(d/'offset-recovery.json',receipt)
            immutable_json(d/f'{sha}.result.json',fixed)
            resurrect_task(db,task_key=key,resurrected_by='01a04040-77ee-77f2-9d26-a5ca1ae56986',
                reason='Explicit unique-source three-span offset projection; original text, fields, failure, and first-pass counts preserved; no gold approval')
            lease=acquire_lease(db,lease_owner='explicit-v5-offset-recovery',lease_seconds=1800,task_key_prefix=key)
            complete_attempt(db,attempt_id=lease['current_attempt_id'],lease_owner=lease['lease_owner'],lease_generation=lease['lease_generation'],output=fixed)
        finally:db.close()


if __name__=='__main__':main()
