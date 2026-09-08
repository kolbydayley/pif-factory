"""Inspected TSMC proposal; no automatic application or semantic approval."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_repair_review_proof import verify


def prepare():
    from scripts import pif_signal_desk_opening_voice_review as review
    verify(review, review.prepare(write=False))
    wid = 'sdw_4acaaec65ea71658126b'
    directory = review.run.OUT / 'calls' / wid / 'A'
    plan = json.loads((review.run.OUT / 'plan.json').read_text())
    source = json.loads((review.run.previous.BASE / f"{plan['source_packets'][wid]}.packet.json").read_text())
    packet = review.run.packet(source, 'A', {})
    review.run.verify_provider(directory, packet)
    original = json.loads((directory / f"{packet['packet_sha256']}.output.json").read_text())
    if digest(original) != review.CASES[wid][1] or packet['packet_sha256'] != review.CASES[wid][0]:
        raise ValueError('original TSMC response changed')
    text = packet['transcript_window']
    changes = []
    def change(path, after, reason):
        value = original
        for part in path: value = value[part]
        changes.append({'path': path, 'before': value, 'after': after, 'reason': reason})
    for index in range(3):
        change(['events', index, 'evidence_role', 'needs'], 'none', 'Readable content; unresolved voice remains null and uncertain, not silently named.')
        change(['events', index, 'evidence_role', 'scope'], 'not_applicable', 'Unlabeled opening does not establish firsthand provenance.')
    change(['events', 3, 'evidence_role', 'needs'], 'none', 'Linked antecedent is intelligible even though the executive remains unnamed.')
    change(['events', 0, 'attitude', 'epistemic'], 'certain', 'Causal failure account is asserted; the counterfactual opportunity hedge is a different proposition.')
    change(['events', 0, 'attitude', 'modality_evidence'], [], 'Do not attach the counterfactual opportunity hedge to the asserted causal failure.')
    change(['events', 0, 'attitude', 'rationale'], 'The causal failure account is asserted and evaluated negatively; the separate counterfactual opportunity does not hedge that account.', 'Align rationale with the independently diagnosed proposition scope.')
    # Source-inspected occurrences, not first/nearest-match relocation.
    locations = [
        (['events', 3, 'attitude', 'target'], 738),
        (['events', 6, 'position', 'source_evidence', 0], 1693),
        (['events', 6, 'position', 'source_evidence', 1], 1980),
        (['events', 9, 'attitude', 'target'], 4044),
        (['events', 10, 'position', 'source_evidence', 1], 4318),
        (['events', 11, 'attitude', 'target'], 4609),
        (['events', 11, 'context_evidence', 0], 4501),
        (['events', 12, 'position', 'source_evidence', 1], 5086)]
    for path, start in locations:
        span = original
        for part in path: span = span[part]
        end = start + len(span['text'])
        if text[start:end] != span['text']: raise ValueError('inspected quote changed')
        change(path + ['start'], start, 'Explicit source-inspected occurrence; quoted text unchanged.')
        change(path + ['end'], end, 'Exact endpoint of that unchanged source occurrence.')
    fixed, proof = propose(original, source=text, window_id=wid, expected_original_sha256=digest(original),
        replacements=changes, output_validator=review.run.previous.contract.validate)
    if fixed['voice_bindings'] != original['voice_bindings']: raise ValueError('voice pool changed')
    proof.update(source_packet_sha256=packet['packet_sha256'], independent_proposal_review_required=True)
    return fixed, proof, packet
