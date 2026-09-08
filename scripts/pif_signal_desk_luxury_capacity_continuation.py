"""Bounded ten-minute recovery after the luxury delta's no-output capacity stop."""
import argparse
import asyncio
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import sys
from types import SimpleNamespace
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_luxury_delta_review as original
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_review_capacity_continuation import prepare as prepare_continuation
parent = SimpleNamespace(OUT=original.OUT, SYSTEM=original.SYSTEM, prepare=original.prepare, run=original.parent.run)
OUT = original.OUT/'capacity-continuation-1'
SYSTEM = original.SYSTEM


def prepare(*, write=True):
    return prepare_continuation(parent,
        failed_sha='4094ab949f7f820cc789a603e5442ec2115ff17c75a502382d1fe57163af6240',
        cooldown_seconds=600, output_root=OUT, write=write)


async def run():
    ps, receipt = prepare()
    delay = max(0, receipt['not_before_epoch']-datetime.now(timezone.utc).timestamp())
    print(json.dumps({'cooldown_seconds_remaining': round(delay), 'remaining_packets': len(ps),
        'preserved_packets': len(receipt['preserved_reviews'])}), flush=True)
    if delay:
        await asyncio.sleep(delay)
    review = parent.run.previous.review
    return await execute(ps, output_root=OUT, task_prefix='luxury-delta-v2-capacity-continuation-1',
        system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
        packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.execute:
            raise SystemExit(asyncio.run(run()))
        print(json.dumps(prepare()[1]))
