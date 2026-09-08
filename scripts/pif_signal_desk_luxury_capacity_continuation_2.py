"""Second bounded retry: twenty-minute backoff and both failure receipts retained."""
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
from scripts import pif_signal_desk_luxury_capacity_continuation as previous
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_review_capacity_continuation import prepare as prepare_continuation
from research_factory.signal_desk_rubric_reference_packets import digest
parent = SimpleNamespace(OUT=previous.OUT, SYSTEM=previous.SYSTEM,
    prepare=lambda write=False: previous.prepare(write=False)[0], run=previous.parent.run)
OUT = previous.OUT.parent/'capacity-continuation-2'
SYSTEM = previous.SYSTEM


def prepare(*, write=True):
    _, prior = previous.prepare(write=False)
    if json.loads((previous.OUT/'plan.json').read_text()) != prior:
        raise ValueError('first capacity lineage changed')
    ps, receipt = prepare_continuation(parent,
        failed_sha='4094ab949f7f820cc789a603e5442ec2115ff17c75a502382d1fe57163af6240',
        cooldown_seconds=1200, output_root=OUT, write=False)
    receipt.update(prior_capacity_lineage_sha256=digest(prior), capacity_failures_preserved=2)
    if write:
        parent.run.immutable_json(OUT/'plan.json', receipt)
        for p in ps:
            parent.run.immutable_json(OUT/f"{p['packet_sha256']}.packet.json", p)
    return ps, receipt


async def run():
    ps, receipt = prepare()
    delay = max(0, receipt['not_before_epoch']-datetime.now(timezone.utc).timestamp())
    print(json.dumps({'cooldown_seconds_remaining': round(delay), 'remaining_packets': len(ps),
        'capacity_failures_preserved': 2}), flush=True)
    if delay:
        await asyncio.sleep(delay)
    review = parent.run.previous.review
    return await execute(ps, output_root=OUT, task_prefix='luxury-delta-v2-capacity-continuation-2',
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
