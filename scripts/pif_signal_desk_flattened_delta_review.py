"""Re-review eleven source-bound corrections; retain all 27 records and unknown voices."""
import argparse
import asyncio
import fcntl
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import pif_signal_desk_flattened_interview_review as parent
from scripts import pif_signal_desk_flattened_capacity_continuation as continuation
from scripts.pif_signal_desk_attribution_probe_review import execute
from research_factory.signal_desk_review_capacity_continuation import combined
from research_factory.signal_desk_full_event_v4_repair import propose
from research_factory.signal_desk_rubric_reference_packets import digest
OUT = parent.OUT.parent / 'flattened-interview-delta-review-v2'
NUMBERS = (8, 9, 12, 14, 15, 16, 18, 19, 22, 24, 25)
SYSTEM = parent.SYSTEM + '\nIndependently check the corrected claims, attitude targets, and epistemic cues. Conditions and discourse fillers are not automatically hedges. Preserve unclear ASR wording and unknown speakers.\n'


def proposal():
    original, prior, p = parent.proposal()
    ds, proofs = combined(continuation)
    expected = {original['events'][n-1]['event_id'] for n in NUMBERS}
    if len(ds) != 27 or {d['event_id'] for d in ds if d['verdict'] != 'supported'} != expected:
        raise ValueError('flattened correction population changed')
    source = p['transcript_window']
    changes = []

    def change(n, path, after, reason):
        path = ['events', n-1] + path
        before = original
        for key in path:
            before = before[key]
        if before != after:
            changes.append(dict(path=path, before=before, after=after, reason=reason))

    def span(text):
        if source.count(text) != 1:
            raise ValueError('source occurrence not unique')
        start = source.index(text)
        return dict(text=text, start=start, end=start+len(text))

    change(8, ['attribution', 'mentioned_entities'], [], 'Google Scholar is a service, not a named organization in this excerpt.')
    change(9, ['claim_text'], 'Examples named for medical LSTM work include the source wording "learning to diagnose ECG analysis," arrhythmia diagnosis, cardiovascular-risk prediction, four-dimensional medical-image segmentation, sleep-stage classification, and COVID detection and prevention.', 'Do not silently resolve awkward ECG wording into a cleaner medical claim.')
    for n in (9, 22, 24):
        change(n, ['attitude', 'epistemic'], 'certain', 'The source asserts the proposition; a discourse filler or stated condition is not an uncertainty cue.')
        change(n, ['attitude', 'modality_evidence'], [], 'Remove filler or condition misclassified as epistemic uncertainty.')
    change(9, ['attitude', 'rationale'], 'The examples are asserted without an epistemic hedge; you know is a discourse filler. No single evaluative target covers the list.', 'Align the explanation with the corrected epistemic classification.')
    change(22, ['attitude', 'rationale'], 'The mechanism is neutrally asserted, with usable higher-level regularity retained as a condition rather than an epistemic hedge.', 'Preserve the condition without conflating it with uncertainty.')
    change(24, ['attitude', 'rationale'], 'The mechanism is neutrally asserted; you know is a discourse filler rather than an epistemic hedge.', 'Do not describe filler as uncertainty.')
    change(12, ['claim_text'], 'The respondent says LLM capabilities are sufficient to facilitate many desktop tasks, including writing summaries of existing documents in a particular style and creating illustrations or an article.', 'Remove stylistic rewriting, which is not separately stated.')
    change(12, ['attitude', 'target'], span('LLMs large language models such\nas ChatGPT.'), 'The positive evaluation applies to the LLM capability, resolved by the existing antecedent.')
    change(14, ['attitude', 'modality_evidence'], [], 'At the moment limits time, not epistemic confidence; current remains in the claim.')
    change(15, ['claim_text'], 'The respondent states that the best chess player has not been human for a quarter century.', 'Remove unsupported reportedly framing while keeping source attribution.')
    change(16, ['attitude', 'target_components', 0, 'target'], span('learning to play chess or\nother board games or video games'), 'Include chess in the complete positively evaluated target.')
    change(18, ['claim_text'], 'The respondent says their group founded NNAISENSE for the physical world, using the transcript wording "in 2014 and 2014" and "eye company".', 'Preserve the duplicated date and unclear company descriptor rather than silently repairing ASR.')
    change(19, ['attitude', 'target'], span('NNAISENSE'), 'The local it resolves to NNAISENSE in the existing antecedent, not some of our projects.')
    change(19, ['attitude', 'modality_evidence'], [span('may have been')], 'Use the actual hedge cue rather than the whole sentence.')
    change(19, ['attitude', 'rationale'], 'NNAISENSE is regretfully evaluated as potentially ahead of its time; may have been explicitly hedges that retrospective evaluation.', 'Align the rationale with the resolved target and hedge.')
    change(25, ['attitude', 'target'], span('predictive world model'), 'Efficient encoding evaluates the predictive world model, not the history being encoded.')
    change(25, ['attitude', 'rationale'], 'The predictive world model is favorably evaluated for efficiently encoding action-observation history.', 'Align the explanation with the evaluated capability.')
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
        raise ValueError('delta coverage changed')
    if write:
        for name, value in [('proposal', fixed), ('provenance', proof)]:
            parent.run.immutable_json(OUT/f'{name}.json', value)
        for q in ps:
            parent.run.immutable_json(OUT/f"{q['packet_sha256']}.packet.json", q)
        parent.run.immutable_json(OUT/'plan.json', dict(packets=[q['packet_sha256'] for q in ps], records=27,
            reviewed_delta_records=11, proposal_sha256=digest(fixed), provenance_sha256=digest(proof),
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
            raise SystemExit(asyncio.run(execute(ps, output_root=OUT, task_prefix='flattened-interview-delta-v2',
                system=SYSTEM, schema_for_packet=review.schema, validator=review.validate_review,
                packet_id=lambda p: p['packet_sha256'], verdict_rows=lambda v: v['decisions'])))
