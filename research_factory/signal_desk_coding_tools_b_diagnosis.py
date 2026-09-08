"""Read-only combined proof; original capacity failure remains immutable."""
import json
from .signal_desk_repair_review_proof import verify
from .signal_desk_rubric_reference_packets import digest


def verified_diagnosis():
    from scripts import pif_signal_desk_coding_tools_b_capacity_retry as retry
    packets, lineage = retry.prepare(write=False)
    # prepare re-verifies original three actual requests, outputs and source
    # grounding. The generic verifier covers the separately metered retry.
    if json.loads((retry.OUT / 'plan.json').read_text()) != lineage:
        raise ValueError('retry lineage changed')
    final, proof = verify(retry, packets)
    rows = []
    for prior in lineage['preserved_reviews']:
        value = json.loads((retry.parent.OUT / f"{prior['packet_sha256']}.review.json").read_text())
        if digest(value) != prior['review_sha256']:
            raise ValueError('preserved review changed')
        rows.extend(value['decisions'])
    rows.extend(final)
    original_packets = retry.parent.prepare(write=False)
    expected = [e['event_id'] for p in original_packets for e in p['candidates']]
    if len(rows) != 14 or [d['event_id'] for d in rows] != expected:
        raise ValueError('combined review population changed')
    return rows, {'preserved_reviews': lineage['preserved_reviews'], 'retry_reviews': proof,
        'original_capacity_failure_sha256': lineage['original_failure_sha256'],
        'records': 14, 'diagnosis_only': True, 'gold_accepted': False, 'applied': False}
