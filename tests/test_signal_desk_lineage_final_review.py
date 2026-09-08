import asyncio
from copy import deepcopy
import pytest
from scripts import pif_signal_desk_lineage_final_review as final
from scripts import pif_signal_desk_lineage_qualification as run
from scripts.pif_signal_desk_lineage_status import inventory
from research_factory import signal_desk_lineage_review as ledger
from test_signal_desk_lineage_qualification import setup,response,save
from test_signal_desk_adjudication_lineage import envelope
from test_signal_desk_full_event_v5 import value,SOURCE


def packet():
    s={'window_id':'dev','transcript_window':SOURCE,'transcript_structure':'speaker_turn'};s['packet_sha256']=run.digest(s)
    return run.packet(s,'C',{'A':value(),'B':value()})


def test_every_input_disposition_including_rejection_gets_reviewed():
    p=packet();v=envelope()
    for row in v['input_dispositions']:row.update(action='rejected',output_event_ids=[],reason='Unsupported candidate.')
    v['records'].update(events=[],voice_bindings=[],window_disposition='no_records')
    ps=ledger.packets(v,p,token_count=lambda text:1)
    assert sum(len(p['candidates']) for p in ps)==2
    assert all(c['disposition']['action']=='rejected' and c['input_record'] for p in ps for c in p['candidates'])


def test_no_source_truncation_to_force_large_packet_through():
    with pytest.raises(ValueError,match='do not truncate'):ledger.packets(envelope(),packet(),token_count=lambda text:12000)


def test_duplicate_review_id_and_invented_quote_rejected():
    p=ledger.packets(envelope(),packet(),token_count=lambda text:1)[0]
    v={'decisions':[{'decision_id':c['decision_id'],'verdict':'supported','rationale':'Equivalent.',
                    'correction':'','source_quotes':[SOURCE]} for c in p['candidates']]}
    ledger.validate(v,p)
    duplicate=deepcopy(v);duplicate['decisions'][1]=deepcopy(duplicate['decisions'][0])
    with pytest.raises(ValueError):ledger.validate(duplicate,p)
    v['decisions'][0]['source_quotes']=['Invented statement.']
    with pytest.raises(ValueError):ledger.validate(v,p)


@pytest.mark.parametrize('missing_last_audit',[False,True])
def test_final_freezes_only_after_all_64_verified_roles(tmp_path,monkeypatch,missing_last_audit):
    plan,out,old=setup(tmp_path,monkeypatch)
    monkeypatch.setattr(final,'OUT',out/'final-review');monkeypatch.setattr(run,'prepare',lambda:plan)
    async def fake(ps,**kwargs):
        p=ps[0];save(kwargs['output_root'],p,response(p));return 0
    monkeypatch.setattr(run,'metered_execute',fake);assert asyncio.run(run.execute(plan))==0
    rows,complete=inventory(plan,require_complete=True);assert len(rows)==64 and len(complete)==16
    assert all(r['original_first_pass_valid'] and not r['repaired_output'] for r in rows)
    if missing_last_audit:
        next((out/'calls'/'w15'/'AUDIT').glob('*.result.json')).unlink()
        with pytest.raises(ValueError,match='incomplete'):final.prepare()
        assert not final.OUT.exists()
    else:
        canonical,lineage=final.prepare()
        assert {p['window_id'] for p in canonical}==set(plan['window_ids'])
        assert sum(len(p['candidates']) for p in lineage)==32
        assert (final.OUT/'plan.json').exists()
