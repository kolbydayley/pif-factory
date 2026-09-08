from copy import deepcopy
import json
import hashlib
import pytest
from scripts import pif_signal_desk_lineage_source_review as review


def fixture():
    p={'cases':[{'case_id':'one'},{'case_id':'two'}],'transcript_window':'A distinct claim. Another claim.'}
    v={'decisions':[{'case_id':i,'verdict':'keep_distinct','rationale':'Different propositions.',
        'source_quotes':['A distinct claim.'],'contract_change':''} for i in ('one','two')]}
    return p,v


def test_exact_review_coverage_and_source_quotes():
    p,v=fixture();assert review.validate(v,p)==v
    s=review.schema(p)['properties']['decisions']
    assert s['minItems']==s['maxItems']==2
    assert s['items']['properties']['case_id']['enum']==['one','two']


@pytest.mark.parametrize('mutation',['missing','duplicate','unknown','quote','empty_quote','verdict','fields','blank_reason'])
def test_invalid_review_fails_closed(mutation):
    p,v=fixture();r=v['decisions'][0]
    if mutation=='missing':v['decisions'].pop()
    elif mutation=='duplicate':v['decisions'][1]=deepcopy(r)
    elif mutation=='unknown':r['case_id']='unassigned'
    elif mutation=='quote':r['source_quotes']=['A invented claim.']
    elif mutation=='empty_quote':r['source_quotes']=['']
    elif mutation=='verdict':r['verdict']='accepted_gold'
    elif mutation=='fields':r['extra']=True
    else:r['rationale']=' '
    with pytest.raises(ValueError):review.validate(v,p)


@pytest.mark.parametrize('tamper',[None,'model','prompt','raw'])
def test_status_checks_actual_provider_request_not_just_valid_json(tmp_path,tamper):
    p,v=fixture();p['packet_sha256']='packet'
    h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    side={'state':'completed','error_class':None,'model':'gpt-5.5','effort':'high',
          'base_instructions_sha256':h(review.SYSTEM),'prompt_sha256':h(json.dumps(p,ensure_ascii=False))}
    if tamper=='model':side['model']='other'
    if tamper=='prompt':side['prompt_sha256']='other'
    raw=deepcopy(v)
    if tamper=='raw':raw['decisions'][0]['verdict']='unresolved'
    for suffix,obj in [('review',v),('output',raw),('sidecar',side)]:
        (tmp_path/f'packet.{suffix}.json').write_text(json.dumps(obj))
    r=review.status([p],tmp_path)
    assert r['all_reviews_verified']==(tamper is None)
    assert not r['gold_accepted'] and r['total_cases']==2
