"""Three independently flagged B records; preserve the original review history."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_tsmc_b_review as parent
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_repair_review_proof import verify
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest

OUT = parent.OUT.parent / 'tsmc-b-explicit-proposal-review-v2'
SYSTEM = parent.SYSTEM + '''\nIndependently review the three corrected records against the full source.
Check executive antecedent context and metadata scope; preserve the transcript's
wording without silently correcting note to node. Separate the leaders' personal
relationship from company-level business, without inventing a causal mechanism.
Prior review findings are not approval of the new candidates. Check all fields.
'''
IDS = {'evt_04', 'evt_06', 'evt_11'}


def proposal():
    baseline, prior, p = parent.proposal()
    decisions, proofs = verify(parent, parent.prepare(write=False))
    if len(decisions) != 13 or {d['event_id'] for d in decisions if d['verdict'] != 'supported'} != IDS:
        raise ValueError('unexpected unresolved B review population')
    changes = []
    def change(path, after, reason):
        before = baseline
        for key in path:
            before = before[key]
        changes.append(dict(path=path, before=before, after=after, reason=reason))
    text = 'So a few years later, the CEO that was put on the new businesses decided that his new assignment wasn’t working out either, so he quit.'
    source = p['transcript_window']
    if source[499:499+len(text)] != text:
        raise ValueError('antecedent source changed')
    change(['events', 3, 'context_evidence'], baseline['events'][3]['context_evidence'] +
           [dict(text=text, start=499, end=499+len(text), purpose='antecedent')], 'Bind the former executive reference to exact preceding context.')
    change(['events', 3, 'evidence_role', 'scope'], 'not_applicable', 'Neutral role metadata is not a strategic attributed view.')
    change(['events', 5, 'claim_text'], "Morris described 40-nanometer execution as important in the progression of Moore's Law and said doing it well was necessary before proceeding to 28 nanometers.", 'Preserve meaning without silently correcting the source word note to node.')
    change(['events', 5, 'attitude', 'rationale'], 'Morris positively evaluates the importance of 40-nanometer execution.', 'Remove the same unsupported lexical normalization from rationale.')
    change(['events', 10, 'claim_text'], 'Ben describes the dispute resolution as successful: Morris and Jensen had a longstanding partnership and close personal relationship, the parties reached a settlement exceeding $100 million, and the companies subsequently did many billions of dollars of business together.', 'Separate personal relationship from corporate business without asserting causality.')
    fixed, proof = propose(baseline, source=source, window_id=p['window_id'], expected_original_sha256=digest(baseline),
                           replacements=changes, output_validator=parent.run.previous.contract.validate)
    proof.update(parent_provenance_sha256=digest(prior), parent_reviews=proofs)
    return fixed, proof, p


def prepare(*, write=True):
    import tiktoken
    fixed, proof, p = proposal()
    enc = tiktoken.get_encoding('o200k_base')
    review = parent.run.previous.review
    all_packets = parent.packets(fixed, source=p['transcript_window'], window_id=p['window_id'],
        token_count=lambda s: len(enc.encode(s)), system=SYSTEM, schema_for_packet=review.schema,
        output_validator=parent.run.previous.contract.validate)
    ps = []
    for original in all_packets:
        candidates = [e for e in original['candidates'] if e['event_id'] in IDS]
        if not candidates:
            continue
        packet = dict(original)
        packet.pop('packet_sha256')
        packet['candidates'] = candidates
        packet.update(proposal_sha256=digest(fixed), repair_proof_sha256=digest(proof))
        packet['schema_sha256'] = digest(review.schema(packet))
        packet['packet_sha256'] = digest(packet)
        if len(enc.encode(SYSTEM + json.dumps(packet, ensure_ascii=False) + json.dumps(review.schema(packet)))) + 1500 > 12000:
            raise ValueError('delta packet exceeds budget')
        ps.append(packet)
    if {e['event_id'] for packet in ps for e in packet['candidates']} != IDS:
        raise ValueError('delta population changed')
    if write:
        OUT.mkdir(parents=True, exist_ok=True, mode=0o700)
        for name, value in [('proposal', fixed), ('provenance', proof)]:
            parent.run.immutable_json(OUT / f'{name}.json', value)
        for packet in ps:
            parent.run.immutable_json(OUT / f"{packet['packet_sha256']}.packet.json", packet)
        parent.run.immutable_json(OUT / 'plan.json', dict(packets=[p['packet_sha256'] for p in ps], records=13,
            reviewed_delta_records=3, proposal_sha256=digest(fixed), provenance_sha256=digest(proof), applied=False, gold_accepted=False))
    return ps


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    with (parent.run.previous.OUT / 'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ps = prepare()
        if args.execute:
            review = parent.run.previous.review
            raise SystemExit(asyncio.run(execute(ps, output_root=OUT, task_prefix='tsmc-b-explicit-proposal-review-v2',
                system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
                packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])))
