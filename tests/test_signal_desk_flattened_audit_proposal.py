import json
from copy import deepcopy
from research_factory.signal_desk_flattened_audit_proposal import prepare
from scripts import pif_signal_desk_flattened_audit_review as review


def test_changes_are_explicit_preserve_all_claims_voices_quotes_and_uncertainty():
    fixed,proof,p=prepare();d=review.run.OUT/'calls'/p['window_id']/'AUDIT'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    expected=deepcopy(raw)
    for change in proof['replacements']:
        target=expected
        for key in change['path'][:-1]:target=target[key]
        assert target[change['path'][-1]]==change['before']
        target[change['path'][-1]]=change['after']
        assert change['path'][-1] in ('needs','start','end')
    assert fixed==expected
    assert fixed['events'][3]==raw['events'][3]
    assert all(e['attribution']['transcript_voice'] is None for e in fixed['events'])
    assert not proof['gold_accepted']


def test_complete_seventeen_record_source_review():
    ps=review.prepare(write=False);fixed,_,p=prepare()
    assert len(fixed['events'])==17
    assert [e['event_id'] for q in ps for e in q['candidates']]==[e['event_id'] for e in fixed['events']]
    assert all(q['transcript_window']==p['transcript_window'] for q in ps)
