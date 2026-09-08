import asyncio
import hashlib
import json
import pytest
from scripts import pif_signal_desk_question_final_review as final
from test_signal_desk_question_qualification import setup, response, save


def test_incomplete_inventory_freezes_nothing(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);monkeypatch.setattr(final,'OUT',out/'final-review')
    with pytest.raises(FileNotFoundError):final.prepare()
    assert not final.OUT.exists()


def test_all_four_roles_and_every_lineage_input_are_reviewed(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);monkeypatch.setattr(final,'OUT',out/'final-review')
    async def fake(ps,**kwargs):
        p=ps[0];save(kwargs['output_root'],p,response(p));return 0
    monkeypatch.setattr(final.run,'metered_execute',fake)
    assert asyncio.run(final.run.execute(plan))==0
    records,ledger,receipt=final.prepare()
    assert len(records)==64
    assert {(p['window_id'],p['author_role']) for p in records}=={(wid,r) for wid in plan['window_ids'] for r in final.run.ROLES}
    assert sum(len(p['candidates']) for p in ledger)==32
    assert len(receipt['inventory'])==16 and not receipt['gold_accepted']
    assert all(p['transcript_window'] for p in records+ledger)


def test_real_receipt_verification_reports_notes_without_accepting_gold(tmp_path,monkeypatch):
    plan,out=setup(tmp_path,monkeypatch);monkeypatch.setattr(final,'OUT',out/'final-review')
    async def fake(ps,**kwargs):
        p=ps[0];save(kwargs['output_root'],p,response(p));return 0
    monkeypatch.setattr(final.run,'metered_execute',fake);asyncio.run(final.run.execute(plan))
    records,ledger,_=final.prepare();h=lambda s:hashlib.sha256(s.encode()).hexdigest()
    for kind,ps,system in [('records',records,final.review.SYSTEM),('ledger',ledger,final.review.LEDGER_SYSTEM)]:
        for p in ps:
            rows=[]
            for c in p['candidates']:
                if kind=='records':rows.append({'event_id':c['event_id'],'verdict':'supported','rationale':'Exact source.',
                    'correction_proposal':'','source_quotes':[p['transcript_window'][:20]]})
                else:rows.append({'decision_id':c['decision_id'],'verdict':'supported','rationale':'Exact source.',
                    'correction':'','source_quotes':[p['transcript_window'][:20]]})
            v={'decisions':rows}
            if kind=='records':v.update(empty_window_verdict='not_applicable',empty_window_rationale='',coverage_notes='')
            if p==records[0]:v['coverage_notes']='Possible missed consequential proposition requires adjudication.'
            s={'state':'completed','error_class':None,'model':'gpt-5.5','effort':'high',
               'base_instructions_sha256':h(system),'prompt_sha256':h(json.dumps(p,ensure_ascii=False))}
            for suffix,value in [('sidecar',s),('output',v),('review',v)]:
                (final.OUT/kind/f"{p['packet_sha256']}.{suffix}.json").write_text(json.dumps(value))
    report=final.verify_results()
    assert report['all_decisions_supported'] and len(report['coverage_adjudications_required'])==1
    assert not report['qualified'] and not report['gold_accepted']
