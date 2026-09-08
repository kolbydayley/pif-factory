"""Second-pass source context/target correction for exactly two flattened records."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_flattened_delta_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_repair_review_proof import verify
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
OUT = parent.OUT.parent/'flattened-interview-context-review-v3'
NUMBERS = (9, 14)
SYSTEM = parent.SYSTEM + '\nCheck the restored medical-LSTM antecedent and the AI capability evaluation. Do not evaluate worker groups in place of AI usefulness, or weaken the expressed only-AI boundary.\n'


def proposal():
    original, prior, p = parent.proposal()
    ds, proofs = verify(parent, parent.prepare(write=False))
    ids = {original['events'][n-1]['event_id'] for n in NUMBERS}
    if len(ds) != 11 or {d['event_id'] for d in ds if d['verdict'] != 'supported'} != ids:
        raise ValueError('flattened follow-up population changed')
    source = p['transcript_window']
    changes = []
    def change(n, path, after, reason):
        path = ['events', n-1]+path
        before = original
        for key in path:
            before = before[key]
        if before != after:
            changes.append(dict(path=path, before=before, after=after, reason=reason))
    def span(text, start):
        if source[start:start+len(text)] != text:
            raise ValueError('source span changed')
        return dict(text=text, start=start, end=start+len(text))
    antecedent = original['events'][7]
    context = span(antecedent['evidence_text'], antecedent['evidence_start'])
    context['purpose'] = 'antecedent'
    change(9, ['context_evidence'], [context], 'Resolve the medical-LSTM scope and it to the preceding literature-search claim.')
    change(14, ['claim_text'], 'The respondent says that, at the moment, the only AI that works well is behind the screen, is good for "desktop to desktop workers," and is not really for people working in the physical world.', 'Preserve the categorical only boundary and the source-specific desktop wording.')
    e = original['events'][13]
    start = e['evidence_start']
    target_start = source.index('AI', start, e['evidence_end'])
    evaluations = []
    for text in ('works\nwell', "it's good", 'not really'):
        at = source.index(text, start, e['evidence_end'])
        evaluations.append(span(text, at))
    change(14, ['attitude', 'target'], span('AI', target_start), 'Both context-dependent evaluations concern AI usefulness, not workers.')
    change(14, ['attitude', 'attitude'], 'mixed', 'The same AI capability is positively evaluated for screen work and negatively for physical-world work.')
    change(14, ['attitude', 'status'], 'proposed', 'Use one mixed evaluation of the same target with explicit context in the claim.')
    change(14, ['attitude', 'evaluation_evidence'], evaluations, 'Ground both sides of the capability evaluation in the source.')
    change(14, ['attitude', 'target_components'], [], 'Remove worker-group targets; they are application contexts rather than evaluated people.')
    change(14, ['attitude', 'rationale'], 'The source evaluates AI capability positively behind the screen and negatively for physical-world work. These are contrasting uses of the same AI target, not judgments of worker groups.', 'Keep the context-specific distinction explicit.')
    fixed, proof = propose(original, source=source, window_id=p['window_id'], expected_original_sha256=digest(original),
        replacements=changes, output_validator=parent.parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior), parent_reviews=proofs)
    return fixed, proof, p


def prepare(*, write=True):
    import tiktoken
    fixed, proof, p = proposal()
    review = parent.parent.run.previous.review
    enc = tiktoken.get_encoding('o200k_base')
    whole = parent.parent.packets(fixed, source=p['transcript_window'], window_id=p['window_id'],
        token_count=lambda s: len(enc.encode(s)), system=SYSTEM, schema_for_packet=review.schema,
        output_validator=parent.parent.run.previous.contract.validate)
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
        raise ValueError('follow-up review coverage changed')
    if write:
        for name, value in [('proposal', fixed), ('provenance', proof)]:
            parent.parent.run.immutable_json(OUT/f'{name}.json', value)
        for q in ps:
            parent.parent.run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json", q)
        parent.parent.run.immutable_json(OUT/'plan.json', dict(packets=[q['packet_sha256'] for q in ps], records=27,
            reviewed_delta_records=2, proposal_sha256=digest(fixed), provenance_sha256=digest(proof),
            applied=False, gold_accepted=False))
    return ps


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (parent.parent.run.previous.OUT/'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ps = prepare()
        if args.execute:
            review = parent.parent.run.previous.review
            raise SystemExit(asyncio.run(execute(ps, output_root=OUT, task_prefix='flattened-interview-context-v3',
                system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
                packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])))
