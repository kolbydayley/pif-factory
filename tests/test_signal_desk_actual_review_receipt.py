import hashlib
import json
import pytest
from research_factory.signal_desk_actual_review_receipt import verify
from research_factory.signal_desk_rubric_reference_packets import digest


def fixture(tmp_path):
    p={'window_id':'dev','transcript_window':'Exact source'};p['packet_sha256']=digest(p)
    h=lambda s:hashlib.sha256(s.encode()).hexdigest();sha=p['packet_sha256']
    s={'state':'completed','error_class':None,'model':'gpt-5.5','effort':'high',
       'base_instructions_sha256':h('system'),'prompt_sha256':h(json.dumps(p,ensure_ascii=False))}
    for suffix,v in [('sidecar',s),('packet',p),('review',{'decisions':[]}),('output',{'decisions':[]})]:
        (tmp_path/f'{sha}.{suffix}.json').write_text(json.dumps(v))
    return p,s


def test_actual_receipt_still_runs_semantic_validator(tmp_path):
    p,_=fixture(tmp_path);seen=[]
    value,proof=verify(tmp_path,p,system='system',validator=lambda v,p:seen.append(v))
    assert seen==[value] and proof['packet_sha256']==p['packet_sha256']


@pytest.mark.parametrize('field,value',[('model','wrong'),('state','in_progress'),('error_class','serverOverloaded'),('prompt_sha256','wrong')])
def test_provider_failure_or_wrong_request_never_counts(tmp_path,field,value):
    p,s=fixture(tmp_path);s[field]=value;(tmp_path/f"{p['packet_sha256']}.sidecar.json").write_text(json.dumps(s))
    with pytest.raises(ValueError,match='provider/request'):verify(tmp_path,p,system='system',validator=lambda v,p:None)


def test_review_projection_and_schema_rejected(tmp_path):
    p,_=fixture(tmp_path)
    def reject(v,p):raise ValueError('schema rejected')
    with pytest.raises(ValueError,match='schema rejected'):verify(tmp_path,p,system='system',validator=reject)
    (tmp_path/f"{p['packet_sha256']}.review.json").write_text('{}')
    with pytest.raises(ValueError,match='projection changed'):verify(tmp_path,p,system='system',validator=lambda v,p:None)
