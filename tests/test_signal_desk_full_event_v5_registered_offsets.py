from copy import deepcopy
import pytest
from research_factory import signal_desk_full_event_v5_registered_offsets as r
from research_factory.signal_desk_rubric_reference_packets import digest
from test_signal_desk_full_event_v5 import value,SOURCE


def fixture(monkeypatch):
    v=value();span=v['events'][0]['position']['source_evidence'][0];span['start']+=1
    p={'window_id':'dev','role':'A','packet_sha256':'fixture','transcript_window':SOURCE}
    entry={'window_id':'dev','role':'A','packet_sha256':'fixture','source_sha256':digest(SOURCE),'records':1,'spans':1,'max_displacement':1}
    monkeypatch.setattr(r,'REGISTRY',{digest(v):entry})
    return v,p,entry


def test_registered_numeric_projection_preserves_every_other_field(monkeypatch):
    v,p,_=fixture(monkeypatch);original=deepcopy(v);fixed,receipt=r.recover(v,p)
    assert v==original and len(fixed['events'])==1
    assert len(receipt['replacements'])==1 and receipt['repaired_span_count']==1
    assert not receipt['semantic_fields_changed'] and not receipt['gold_accepted']
    assert fixed['events'][0]['claim_text']==v['events'][0]['claim_text']


@pytest.mark.parametrize('mutation',['raw','source','role','packet','bound','population'])
def test_uninspected_inputs_fail_closed(monkeypatch,mutation):
    v,p,e=fixture(monkeypatch)
    if mutation=='raw':v['events'][0]['claim_text']+=' changed'
    elif mutation=='source':p['transcript_window']+=' changed'
    elif mutation=='role':p['role']='B'
    elif mutation=='packet':p['packet_sha256']='wrong'
    elif mutation=='bound':e['max_displacement']=0
    else:e['spans']=2
    with pytest.raises(ValueError):r.recover(v,p)


def test_even_registered_text_cannot_choose_between_duplicate_occurrences(monkeypatch):
    v,p,e=fixture(monkeypatch);p['transcript_window']=SOURCE+SOURCE;e['source_sha256']=digest(p['transcript_window'])
    with pytest.raises(ValueError,match='ambiguous'):r.recover(v,p)
