from copy import deepcopy
import json
import pytest
from research_factory import signal_desk_full_event_v5_offset_recovery as recovery
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v5 import value,SOURCE


def fixture(monkeypatch):
    v=value();e=v['events'][0]
    v['events']=[dict(deepcopy(e),event_id=f'c{i}') for i in range(9)]
    path=['events',0,'position','source_evidence',0]
    span=v
    for k in path:span=span[k]
    start,end=span['start'],span['end'];text=span['text']
    span['start']+=1
    monkeypatch.setattr(recovery,'WID','dev')
    monkeypatch.setattr(recovery,'INSPECTED',[(path,text,start+1,end,start,end)])
    return v


def test_only_explicit_offsets_change_and_result_is_reproducible(tmp_path,monkeypatch):
    v=fixture(monkeypatch);old=deepcopy(v)
    fixed,r=recovery.recover(v,source=SOURCE,window_id='dev')
    assert old==v and len(fixed['events'])==9 and not r['semantic_fields_changed']
    p={'packet_sha256':'p','transcript_window':SOURCE,'window_id':'dev'}
    for name,data in [('p.output.json',v),('p.result.json',fixed),('offset-recovery.json',r)]:
        (tmp_path/name).write_text(json.dumps(data))
    result,provenance=recovery.load_call(tmp_path,p)
    assert result==fixed and provenance['offset_recovery']
    r['gold_accepted']=True;(tmp_path/'offset-recovery.json').write_text(json.dumps(r))
    with pytest.raises(ValueError,match='unverified'):recovery.load_call(tmp_path,p)


def test_no_ambiguous_or_changed_span_repair(monkeypatch):
    v=fixture(monkeypatch)
    with pytest.raises(ValueError,match='unique source'):recovery.recover(v,source=SOURCE+SOURCE,window_id='dev')
    v['events'][0]['position']['source_evidence'][0]['text']='invented'
    with pytest.raises(ValueError,match='span changed'):recovery.recover(v,source=SOURCE,window_id='dev')
