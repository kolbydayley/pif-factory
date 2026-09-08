"""Unapproved C delta restoring the independently identified lost mechanism."""
from copy import deepcopy
from .signal_desk_tsmc_c_proposal import prepare as baseline
from .signal_desk_rubric_reference_packets import digest


def prepare():
    from scripts import pif_signal_desk_tsmc_c_review as review
    from .signal_desk_actual_review_receipt import verify
    original, prior, packet = baseline()
    _, ledger, _ = review.prepare(write=False)
    target = next(p for p in ledger if any(c['decision_id'] == 'input-0012' for c in p['candidates']))
    verdict, receipt = verify(review.OUT/'ledger', target, system=review.ledger.SYSTEM,
                              validator=review.ledger.validate)
    decision = next(d for d in verdict['decisions'] if d['decision_id'] == 'input-0012')
    if decision['verdict'] != 'material_error':
        raise ValueError('expected independently identified consultation omission')
    source = packet['transcript_window']
    prefix = 'I called the salespeople that happened in direct contact with NVIDIA. Of course, I called everybody that was somewhat involved in the problem.'
    if source.count(prefix) != 1:
        raise ValueError('consultation source occurrence changed')
    fixed = deepcopy(original)
    event = fixed['records']['events'][7]
    if event['event_id'] != 'evt_08_nvidia_damage_resolution_mechanism':
        raise ValueError('mechanism identity changed')
    event['claim_text'] = ('TSMC was already doing what it could on manufacturing for its broader needs, '
        'but because NVIDIA bore the brunt of the damage, Morris treated the remaining dispute as monetary. '
        'He consulted salespeople in direct contact with NVIDIA and others involved, reviewed the problem '
        'and NVIDIA customers’ demands, and calculated a figure.')
    event['evidence_start'] = source.index(prefix)
    event['evidence_text'] = source[event['evidence_start']:event['evidence_end']]
    event['position']['source_evidence'].insert(0, dict(text=prefix, start=source.index(prefix),
                                                      end=source.index(prefix)+len(prefix)))
    fixed['input_dispositions'][12]['reason'] = ('Preserves the personnel consultation, operational remediation, '
        'customer damage and monetary calculation in the same source-bound mechanism record.')
    review.run.validate(fixed, packet)
    proof = dict(parent_proposal_sha256=digest(original), parent_provenance_sha256=digest(prior),
                 correction_review=receipt, proposed_envelope_sha256=digest(fixed),
                 changed_event_id=event['event_id'], changed_disposition='input-0012',
                 independent_record_and_affected_lineage_review_required=True,
                 applied=False, gold_accepted=False)
    return fixed, proof, packet
