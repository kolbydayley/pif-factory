import hashlib
import json
from types import SimpleNamespace
import pytest
from research_factory import signal_desk_reviewed_need_recovery as recovery
from research_factory import signal_desk_full_event_v5_review as validator


@pytest.mark.parametrize('tamper',[None,'missing','verdict','model','request','raw','proposal'])
def test_recovery_requires_frozen_proposal_and_actual_supported_approval(tmp_path,monkeypatch,tamper):
    original={'events':[{'event_id':str(i)} for i in range(11)]};fixed={'events':[{'event_id':str(i)} for i in range(11)]}
    proof={'original_sha256':recovery.digest(original)}
    p={'window_id':'w','role':'A','packet_sha256':'source','transcript_window':'Actual source.'}
    request={'packet_sha256':'review','transcript_window':p['transcript_window'],'candidates':[{'event_id':'1'}]}
    module=SimpleNamespace(OUT=tmp_path,WID='w',EID='1',SYSTEM='frozen system',prepare=lambda **kwargs:([request],fixed,proof))
    monkeypatch.setattr(recovery,'case_module',lambda case:module)
    # Envelope/source validation has its own tests; here isolate proof-chain
    # checks without manufacturing a complete semantic reviewer response.
    monkeypatch.setattr(validator,'validate_review',lambda v,p:v)
    result={'decisions':[{'event_id':'1','verdict':'supported'}]};h=lambda text:hashlib.sha256(text.encode()).hexdigest()
    side={'state':'completed','error_class':None,'model':'gpt-5.5','effort':'high',
          'base_instructions_sha256':h(module.SYSTEM),'prompt_sha256':h(json.dumps(request,ensure_ascii=False))}
    docs={'plan.json':{'packets':['review'],'source_packet_sha256':'source'},'proposal.json':fixed,'provenance.json':proof,
          'review.packet.json':request,'review.review.json':result,'review.output.json':result,'review.sidecar.json':side}
    for name,value in docs.items():(tmp_path/name).write_text(json.dumps(value))
    if tamper=='missing':(tmp_path/'review.review.json').unlink()
    elif tamper=='verdict':
        result['decisions'][0]['verdict']='unresolved'
        for name in ['review.review.json','review.output.json']:(tmp_path/name).write_text(json.dumps(result))
    elif tamper in {'model','request'}:
        side['model' if tamper=='model' else 'prompt_sha256']='wrong';(tmp_path/'review.sidecar.json').write_text(json.dumps(side))
    elif tamper=='raw':(tmp_path/'review.output.json').write_text('{}')
    elif tamper=='proposal':(tmp_path/'proposal.json').write_text('{}')
    if tamper:
        with pytest.raises((ValueError,FileNotFoundError)):recovery.recover('hidden-brain',original,p)
    else:
        value,receipt=recovery.recover('hidden-brain',original,p)
        assert value==fixed and receipt['records_after']==11 and not receipt['gold_accepted']
