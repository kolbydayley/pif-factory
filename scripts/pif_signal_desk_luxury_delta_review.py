"""Re-review four luxury records with source context and uncertainty preserved."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_luxury_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_repair_review_proof import verify
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
OUT = parent.OUT.parent/'luxury-delta-review-v2'
NUMBERS = (1, 2, 4, 10)
SYSTEM = parent.SYSTEM + '\nReview the complete corrected records, including ambiguous financial denominators and attitude valence. Evidence context is not an additional claim or a new speaker assignment.\n'


def proposal():
    original, prior, p = parent.proposal()
    ds, proofs = verify(parent, parent.prepare(write=False))
    expected = {original['events'][n-1]['event_id'] for n in NUMBERS}
    if len(ds) != 15 or {d['event_id'] for d in ds if d['verdict'] != 'supported'} != expected:
        raise ValueError('luxury correction population changed')
    source = p['transcript_window']
    changes = []
    def change(n, path, after, reason):
        path = ['events', n-1]+path
        before = original
        for key in path:
            before = before[key]
        if before != after:
            changes.append(dict(path=path, before=before, after=after, reason=reason))
    def span(text, purpose=None):
        if source.count(text) != 1:
            raise ValueError('source occurrence changed')
        start = source.index(text)
        result = dict(text=text, start=start, end=start+len(text))
        if purpose:
            result['purpose'] = purpose
        return result
    change(1, ['attitude', 'attitude'], 'indeterminate', 'Magnitude and consumer notice do not establish negative valence.')
    change(1, ['attitude', 'status'], 'needs_review', 'The polarity is not resolved by the source.')
    change(1, ['attitude', 'rationale'], 'The speaker asserts a large increase and consumer notice, but the magnitude phrase does not clearly establish positive or negative valence.', 'Preserve indeterminate valence rather than infer criticism.')
    discount = "it's not like I go to wherever and I can buy something 50% off this is not going\nto 50% off it's training the consumer a certain mindset"
    change(2, ['context_evidence'], [span(discount, 'supporting_context')], 'Include the explicit discounting contrast supporting the recommendation.')
    change(4, ['claim_text'], 'Making the product is described as probably 30%, with the denominator unclear in the transcript; the speaker also cites gross margins near 70% and substantial support costs.', 'Do not assert a clean cost denominator or a reconciled margin calculation.')
    change(4, ['attitude', 'target'], span('business by itself'), 'Use the evaluated business phrase, not a generic word at the question boundary.')
    change(4, ['evidence_role', 'rationale'], 'The passage gives approximate production economics, but the 30% denominator is unclear; the support-cost caveat and margin figure are retained without imposing a calculation.', 'Expose the uncertainty in the quantitative interpretation.')
    start = source.index('with size comes better scale so Gucci had higher margins')
    end = original['events'][9]['evidence_start']
    change(10, ['context_evidence'], [span(source[start:end].rstrip(), 'antecedent')], 'Resolve it and their business to Gucci using the immediately preceding revenue discussion.')
    fixed, proof = propose(original, source=source, window_id=p['window_id'], expected_original_sha256=digest(original),
        replacements=changes, output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior), parent_reviews=proofs)
    return fixed, proof, p


def prepare(*, write=True):
    import tiktoken
    fixed, proof, p = proposal()
    review = parent.run.previous.review
    enc = tiktoken.get_encoding('o200k_base')
    whole = parent.packets(fixed, source=p['transcript_window'], window_id=p['window_id'],
        token_count=lambda s: len(enc.encode(s)), system=SYSTEM, schema_for_packet=review.schema,
        output_validator=parent.run.previous.contract.validate)
    expected = {fixed['events'][n-1]['event_id'] for n in NUMBERS}
    ps = []
    for row in whole:
        candidates = [e for e in row['candidates'] if e['event_id'] in expected]
        if not candidates:
            continue
        q = dict(row)
        q.pop('packet_sha256')
        q['candidates'] = candidates
        q.update(proposal_sha256=digest(fixed), repair_proof_sha256=digest(proof))
        q['schema_sha256'] = digest(review.schema(q))
        q['packet_sha256'] = digest(q)
        ps.append(q)
    if {e['event_id'] for q in ps for e in q['candidates']} != expected:
        raise ValueError('luxury delta coverage changed')
    if write:
        for name, value in [('proposal', fixed), ('provenance', proof)]:
            parent.run.immutable_json(OUT/f'{name}.json', value)
        for q in ps:
            parent.run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json", q)
        parent.run.immutable_json(OUT/'plan.json', dict(packets=[q['packet_sha256'] for q in ps], records=15,
            reviewed_delta_records=4, proposal_sha256=digest(fixed), provenance_sha256=digest(proof),
            applied=False, gold_accepted=False))
    return ps


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ps = prepare()
        if args.execute:
            review = parent.run.previous.review
            raise SystemExit(asyncio.run(execute(ps, output_root=OUT, task_prefix='luxury-delta-v2',
                system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
                packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])))
