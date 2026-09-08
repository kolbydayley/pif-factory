import hashlib
import json
import pytest
from scripts import pif_signal_desk_coding_tools_repair_review as module
from research_factory.signal_desk_coding_tools_recovery import recover


def fixture(tmp_path):
    if not (module.diagnosis.OUT/'plan.json').exists():pytest.skip('private development fixture absent')
    ps,fixed,proof=module.prepare(write=False)
    def save(name,v):(tmp_path/name).write_text(json.dumps(v))
    save('proposal.json',fixed);save('provenance.json',proof)
    save('plan.json',{'packets':[p['packet_sha256'] for p in ps],'proposal_sha256':module.digest(fixed),'provenance_sha256':module.digest(proof)})
    h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    for p in ps:
        sha=p['packet_sha256'];save(f'{sha}.packet.json',p)
        v={'decisions':[{'event_id':e['event_id'],'verdict':'supported','correction_proposal':'','rationale':'Fixture verdict.',
                        'source_quotes':[p['transcript_window'][:30]]} for e in p['candidates']],
           'empty_window_verdict':'not_applicable','empty_window_rationale':'','coverage_notes':''}
        for suffix in ('review','output'):save(f'{sha}.{suffix}.json',v)
        save(f'{sha}.sidecar.json',{'state':'completed','model':'gpt-5.5','effort':'high','base_instructions_sha256':h(module.SYSTEM),
                                  'prompt_sha256':h(json.dumps(p,ensure_ascii=False))})
    d=module.diagnosis.run.OUT/'calls'/module.diagnosis.WID/'A'
    p=json.loads((d/'packet.json').read_text());raw=json.loads((d/f'{module.diagnosis.PACKET}.output.json').read_text())
    return ps,p,raw


@pytest.mark.parametrize('mutation',['none','missing','wrong_model','unsupported','raw_change'])
def test_all_packet_approval_is_mandatory(tmp_path,mutation):
    ps,p,raw=fixture(tmp_path);sha=ps[-1]['packet_sha256']
    if mutation=='missing':(tmp_path/f'{sha}.review.json').unlink()
    elif mutation=='wrong_model':
        f=tmp_path/f'{sha}.sidecar.json';v=json.loads(f.read_text());v['model']='wrong';f.write_text(json.dumps(v))
    elif mutation=='unsupported':
        f=tmp_path/f'{sha}.review.json';v=json.loads(f.read_text());v['decisions'][0].update(verdict='needs_correction',correction_proposal='Fix meaning.')
        for suffix in ('review','output'):(tmp_path/f'{sha}.{suffix}.json').write_text(json.dumps(v))
    elif mutation=='raw_change':raw['events'][0]['claim_text']='tampered'
    if mutation=='none':
        fixed,proof=recover(raw,p,review_root=tmp_path)
        assert len(fixed['events'])==15 and len(proof['reviews'])==len(ps) and not proof['gold_accepted']
    else:
        with pytest.raises((ValueError,OSError)):recover(raw,p,review_root=tmp_path)
