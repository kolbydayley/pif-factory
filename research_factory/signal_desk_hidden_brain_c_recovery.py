"""Reconstruct only the independently approved three-field C correction."""
import json
from .signal_desk_rubric_reference_packets import digest
from .signal_desk_repair_review_proof import verify


def recover(original, packet):
    from scripts import pif_signal_desk_hidden_brain_c_repair_review as review
    packets, fixed, proof = review.prepare(write=False)
    if digest(original) != proof['original_envelope_sha256'] or packet['packet_sha256'] != proof['source_packet_sha256']:
        raise ValueError('original C or packet changed')
    if json.loads((review.OUT / 'proposal.json').read_text()) != fixed or json.loads((review.OUT / 'provenance.json').read_text()) != proof:
        raise ValueError('saved proposal changed')
    decisions, reviews = verify(review, packets)
    expected = {review.WID + '_c2', review.WID + '_c11'}
    if {d['event_id'] for d in decisions} != expected or len(decisions) != 2 or any(d['verdict'] != 'supported' for d in decisions):
        raise ValueError('C repair lacks independent support')
    if original['input_dispositions'] != fixed['input_dispositions'] or original['additions'] != fixed['additions']:
        raise ValueError('lineage changed')
    return fixed, {'repair': proof, 'reviews': reviews, 'gold_accepted': False, 'qualified': False}
