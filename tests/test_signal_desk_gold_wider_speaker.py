from __future__ import annotations
import json,sqlite3
import pytest
from research_factory.signal_desk_gold_wider_speaker import *
from research_factory.signal_desk_rebuild_dispatch import acquire_lease,fail_attempt_semantically,resurrect_task

def packet(mentioned=(),quoted=None,text='Alice Smith says words'):
 candidate={'window_id':'w','events':[{'id':'e','mentioned':list(mentioned),'quoted':quoted}]}
 p={'candidate':candidate,'context':{'context_text':text},'packet_sha256':'a'*64};return p
def output(speaker='Alice Smith',action='accept',decision='supported_speaker'):
 return {'model':'gpt-5.5','window_id':'w','packet_sha256':'a'*64,'action':action,'rationale':'grounded','decisions':[{'event_id':'e','decision':decision,'speaker_surface':speaker if decision=='supported_speaker' else None,'provenance_start':0 if decision=='supported_speaker' else None,'provenance_end':len(speaker) if decision=='supported_speaker' else None}]}

def test_supported_surface_exact(): assert validate_decision(output(),packet())['decisions'][0]['speaker_surface']=='Alice Smith'
@pytest.mark.parametrize('field', ['mentioned','quoted'])
def test_third_party_and_quoted_people_cannot_become_speaker(field):
 p=packet(mentioned=['Alice Smith'] if field=='mentioned' else (),quoted='Alice Smith' if field=='quoted' else None)
 with pytest.raises(WiderSpeakerError,match='quoted or mentioned'):validate_decision(output(),p)
def test_generic_clip_label_fails_closed():
 with pytest.raises(WiderSpeakerError,match='exact supported surface'):validate_decision(output('Army Film Clip'),packet(text='Army Film Clip speaks'))
def test_second_wider_request_fails_closed():
 with pytest.raises(WiderSpeakerError,match='second wider'):validate_decision({'model':'gpt-5.5','window_id':'w','packet_sha256':'a'*64,'action':'request_wider_context','rationale':'need more','decisions':[]},packet())
def test_indeterminable_has_no_identity():
 assert validate_decision(output(action='accept',decision='indeterminable'),packet())['decisions'][0]['decision']=='indeterminable'
def test_dispatch_is_idempotent_and_resurrectable(tmp_path):
 private=tmp_path/'private';private.mkdir();(private/'w.packet.json').write_text('{}')
 plan={'packets':[{'window_id':'w','packet_sha256':'x'}]};db=tmp_path/'d.sqlite'
 assert enqueue_packets(dispatch_path=db,plan=plan,private_root=private)=={'created':1}
 assert enqueue_packets(dispatch_path=db,plan=plan,private_root=private)=={'existing':1}
 conn=sqlite3.connect(db);conn.row_factory=sqlite3.Row
 lease=acquire_lease(conn,lease_owner='test',lease_seconds=60,task_key_prefix=TASK_PREFIX)
 fail_attempt_semantically(conn,attempt_id=lease['current_attempt_id'],lease_owner='test',lease_generation=lease['lease_generation'],failure_code='invalid',failure_detail='test')
 assert enqueue_packets(dispatch_path=db,plan=plan,private_root=private)=={'existing_terminal_failed':1}
 revived=resurrect_task(conn,task_key=TASK_PREFIX+'w',resurrected_by='test',reason='corrected packet review')
 assert revived['attempt_number']==2 and revived['attempt_status']=='pending';conn.close()
def test_contract_constants():
 assert MODEL=='gpt-5.5' and EXPECTED_CALLS==9 and RESERVE_TOKENS_PER_CALL*EXPECTED_CALLS==450_000
 assert CALL_DEADLINE_SECONDS==900 and LEASE_SECONDS==1800 and MAX_CONCURRENCY==2

def test_per_call_reservation_is_settled_to_actual_usage(monkeypatch,tmp_path):
 import research_factory.signal_desk_gold_wider_speaker as lane
 conn=sqlite3.connect(':memory:')
 monkeypatch.setattr(lane,'rebuild_budget_gate',lambda *a,**k:{'allowed':True,'remaining_tokens':1_000_000,'reason':None})
 monkeypatch.setattr(lane,'record_usage',lambda *a,**k:'ledger-1')
 budget=ApprovalBudgetConfig(campaign_id=CAMPAIGN_ID,grant_path=tmp_path/'grant',budget_dir=tmp_path/'budget')
 reservation=reserve_call(conn,task_key='task:generation:1',budget=budget)
 assert reservation['reserved_tokens']==50_000 and reservation['status']=='reserved'
 settled=settle_call(conn,reservation_id=reservation['reservation_id'],task_key='task:generation:1',actual_tokens=12_345)
 assert settled=={'charged_tokens':12_345,'status':'settled','ledger_id':'ledger-1'}
 assert conn.execute('select reserved_tokens,actual_tokens,status from signal_desk_wider_speaker_reservations').fetchone()==(50_000,12_345,'settled')
