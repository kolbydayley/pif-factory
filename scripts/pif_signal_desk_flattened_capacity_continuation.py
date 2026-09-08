"""Retry only the no-output final flattened-review capacity failure."""
import argparse
import asyncio
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_flattened_interview_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_review_capacity_continuation import prepare as prepare_continuation
OUT = parent.OUT / 'capacity-continuation-1'
SYSTEM = parent.SYSTEM


def prepare(*, write=True):
    return prepare_continuation(parent,
        failed_sha='783b6ff0cf736082b6b9bcb5506db5964f633dff363a154ec7dd82118d5b400b',
        cooldown_seconds=300, output_root=OUT, write=write)


async def run():
    ps, receipt = prepare()
    delay = max(0, receipt['not_before_epoch'] - datetime.now(timezone.utc).timestamp())
    print(json.dumps({'cooldown_seconds_remaining': round(delay),
        'remaining_packets': len(ps), 'preserved_packets': len(receipt['preserved_reviews'])}), flush=True)
    if delay:
        await asyncio.sleep(delay)
    review = parent.run.previous.review
    return await execute(ps, output_root=OUT, task_prefix='flattened-interview-review-v1-capacity-continuation-1',
        system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
        packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (parent.run.previous.OUT / 'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.execute:
            raise SystemExit(asyncio.run(run()))
        print(json.dumps(prepare()[1]))
