from copy import deepcopy
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
