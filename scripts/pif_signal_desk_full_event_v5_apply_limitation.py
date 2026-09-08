#!/usr/bin/env python3
"""Apply only the source-reconciled, independently supported B quarantine repair."""
import argparse
import fcntl
import json
import sqlite3
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts.pif_signal_desk_full_event_v5_limitation_review import run,OUT,WID
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_full_event_v5_limitation_recovery import recover
from research_factory.signal_desk_rebuild_dispatch import resurrect_task,acquire_lease,complete_attempt


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    with (run.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        plan=run.prepare();source=json.loads((run.BASE/f"{plan['source_packets'][WID]}.packet.json").read_text())
        p=run.author.packet(source,'B');d=run.OUT/'calls'/WID/'B';sha=p['packet_sha256']
        if json.loads((d/'packet.json').read_text())!=p:raise ValueError('B source/contract changed')
        raw=json.loads((d/f'{sha}.output.json').read_text());fixed,proof=recover(raw,p,OUT)
        print(json.dumps(proof),flush=True)
        if not args.execute:return
        db=sqlite3.connect(d/'dispatch.sqlite');db.row_factory=sqlite3.Row
        try:
            key='full-event-v5-context-qualification:'+sha
            row=db.execute('SELECT a.* FROM signal_desk_rebuild_tasks t JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id WHERE t.task_key=?',(key,)).fetchone()
            if row is None or row['status']!='terminal_failed' or row['semantic_failure_detail']!='source limitation requires recovery need':
                raise ValueError('not the inspected B failure')
            immutable_json(d/'reviewed-repair.json',proof);immutable_json(d/f'{sha}.result.json',fixed)
            resurrect_task(db,task_key=key,resurrected_by='01a04040-77ee-77f2-9d26-a5ca1ae56986',
                reason='Source-reconciled GPT-5.5 supported quarantine of one underspecified record; original fields/failure and all nine records retained; not accepted gold')
            lease=acquire_lease(db,lease_owner='reviewed-v5-limitation-repair',lease_seconds=1800,task_key_prefix=key)
            complete_attempt(db,attempt_id=lease['current_attempt_id'],lease_owner=lease['lease_owner'],lease_generation=lease['lease_generation'],output=fixed)
        finally:db.close()


if __name__=='__main__':main()
