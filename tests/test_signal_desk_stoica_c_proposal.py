import json
from copy import deepcopy
from research_factory.signal_desk_stoica_c_proposal import prepare
from scripts import pif_signal_desk_stoica_c_review as review


def test_only_numeric_offset_changes_and_full_lineage_reviewed():
    fixed,proof,p=prepare()
    d=review.run.OUT/'calls'/p['window_id']/'C'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    restored=deepcopy(fixed)
    restored['records']['events'][8]['position']['source_evidence'][0]['start']=3917
    assert restored==raw
    assert proof['gold_accepted'] is False
    records,lineage,plan=review.prepare(write=False)
    assert sum(len(p['candidates']) for p in records)==15
    assert sum(len(p['candidates']) for p in lineage)==27
    assert plan['additions']==0
    assert all(q['transcript_window']==p['transcript_window'] for q in records+lineage)
