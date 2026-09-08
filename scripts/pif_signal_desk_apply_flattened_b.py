"""Preserve original audit failure and raw output while applying a reviewed projection."""
import argparse
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_lineage_qualification as lane
from scripts.pif_signal_desk_apply_need_repairs import apply_verified
from research_factory.signal_desk_flattened_b_recovery import recover
from research_factory.signal_desk_flattened_b_proposal import prepare


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (lane.previous.parent.OUT/'runner.lock').open('a') as a, (lane.previous.OUT/'runner.lock').open('a') as b:
        for lock in (a, b):
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _, _, p = prepare()
        d = lane.OUT/'calls'/p['window_id']/'B'
        raw = json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
        fixed, proof = recover(raw, p)
        result = apply_verified(d, p, fixed, proof, 'full-event-lineage-qualification-v1', execute=args.execute,
            expected_failure='span not exact source', receipt_name='flattened-b-repair.json')
        print(json.dumps({k: v for k, v in result.items() if k != 'proof'}))

