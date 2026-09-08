from scripts import pif_signal_desk_tsmc_repair_review as review
from research_factory.signal_desk_rubric_reference_packets import digest


def test_all_records_full_source_and_exact_proposal():
    ps=review.prepare(write=False)
    value,_,source=review.proposal()
    assert [e['event_id'] for p in ps for e in p['candidates']]==[e['event_id'] for e in value['events']]
    assert all(p['transcript_window']==source['transcript_window'] for p in ps)
    assert all(p['original_output_sha256']==digest(value) for p in ps)
    assert all(not p['gold_accepted'] for p in ps)
