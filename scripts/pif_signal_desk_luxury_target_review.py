"""Independently review the remaining neutral analytical target correction."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_luxury_delta_review as parent
from scripts import pif_signal_desk_luxury_capacity_continuation_2 as continuation
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_review_capacity_continuation import combined
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
OUT = parent.OUT.parent/'luxury-target-review-v3'
SYSTEM = parent.SYSTEM + '\nReview the complete remaining record with its source context. A neutral analytical financial statement need not have an evaluative target.\n'


def proposal():
    original, prior, p = parent.proposal()
    rows, proofs = combined(continuation)
    if len(rows) != 4 or {d['event_id'] for d in rows if d['verdict'] != 'supported'} != {'evt-10'}:
        raise ValueError('luxury target correction population changed')
    change = dict(path=['events', 9, 'attitude', 'target'],
        before=original['events'][9]['attitude']['target'], after=None,
        reason='Neutral financial analysis has no explicit evaluative target; the margin metric is not one.')
    fixed, proof = propose(original, source=p['transcript_window'], window_id=p['window_id'],
        expected_original_sha256=digest(original), replacements=[change],
        output_validator=parent.parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior), parent_reviews=proofs)
    return fixed, proof, p


def prepare(*, write=True):
    import tiktoken
    fixed, proof, p = proposal()
    run = parent.parent.run
    review = run.previous.review
    enc = tiktoken.get_encoding('o200k_base')
    whole = parent.parent.packets(fixed, source=p['transcript_window'], window_id=p['window_id'],
        token_count=lambda s: len(enc.encode(s)), system=SYSTEM, schema_for_packet=review.schema,
        output_validator=run.previous.contract.validate)
    ps = []
    for row in whole:
        candidates = [e for e in row['candidates'] if e['event_id'] == 'evt-10']
        if not candidates:
            continue
        q = dict(row)
        q.pop('packet_sha256')
        q['candidates'] = candidates
        q.update(proposal_sha256=digest(fixed), repair_proof_sha256=digest(proof))
        q['schema_sha256'] = digest(review.schema(q))
        q['packet_sha256'] = digest(q)
        ps.append(q)
    if len(ps) != 1 or len(ps[0]['candidates']) != 1:
        raise ValueError('luxury target coverage changed')
    if write:
        for name, value in [('proposal', fixed), ('provenance', proof)]:
            run.immutable_json(OUT/f'{name}.json', value)
        for q in ps:
            run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json", q)
        run.immutable_json(OUT/'plan.json', dict(packets=[q['packet_sha256'] for q in ps], records=15,
            reviewed_delta_records=1, proposal_sha256=digest(fixed), provenance_sha256=digest(proof),
            applied=False, gold_accepted=False))
    return ps


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    run = parent.parent.run
    with (run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ps = prepare()
        if args.execute:
            review = run.previous.review
            raise SystemExit(asyncio.run(execute(ps, output_root=OUT, task_prefix='luxury-target-v3',
                system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
                packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])))
