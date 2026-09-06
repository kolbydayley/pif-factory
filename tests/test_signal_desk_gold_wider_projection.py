from __future__ import annotations
import copy
from research_factory.signal_desk_gold_repair_v4 import _sha_json
from research_factory.signal_desk_gold_wider_projection import build_resurrection_receipt,project_window
from research_factory.signal_desk_rebuild_dispatch import acquire_lease,enqueue_task,fail_attempt_semantically,initialize_dispatch_schema

def payload():
 event={'event_id':'w_001','claim_text':'A claim.','speech_act':'assertion','evidence_text':'words','evidence_start':0,'evidence_end':5,'speaker_id':None,'quoted_person_id':None,'mentioned_person_ids':[],'attribution_type':'unresolved_speaker','attribution_confidence':1.0,'issue_label':'issue','issue_aliases':[],'stance':'neutral','publishability_state':'candidate'}
 base={'schema_version':'pif_signal_desk_clean_event_v2','window_id':'w','window_disposition':'claims_found','events':[event]}
 v5=copy.deepcopy(base);v5['events']=[];v5['window_disposition']='no_consequential_claims'
 provenance={'unresolved':[{'event_id':'w_001','event_sha256':_sha_json(event)}]}
 return base,v5,provenance

def test_supported_decision_restores_only_attribution_fields():
 base,v5,provenance=payload(); decision={'decisions':[{'event_id':'w_001','decision':'supported_speaker','speaker_surface':'Alice'}]}
 out,receipt=project_window(baseline_c=base,v5_c=v5,v5_provenance=provenance,decision=decision,transcript_window='words')
 assert out['events'][0]['speaker_id']=='Alice';assert out['events'][0]['claim_text']=='A claim.'
 assert receipt['restored_count']==1 and receipt['residual_count']==0

def test_indeterminable_stays_quarantined():
 base,v5,provenance=payload();decision={'decisions':[{'event_id':'w_001','decision':'indeterminable','speaker_surface':None}]}
 out,receipt=project_window(baseline_c=base,v5_c=v5,v5_provenance=provenance,decision=decision,transcript_window='words')
 assert out['events']==[] and receipt['indeterminable_count']==1 and receipt['residual_count']==1

def test_digest_mismatch_fails_closed():
 import pytest
 base,v5,provenance=payload();provenance['unresolved'][0]['event_sha256']='bad'
 with pytest.raises(RuntimeError,match='not bound'):
  project_window(baseline_c=base,v5_c=v5,v5_provenance=provenance,decision={'decisions':[{'event_id':'w_001','decision':'supported_speaker','speaker_surface':'Alice'}]},transcript_window='words')

def test_resurrection_requires_direct_identity_proof_and_preserves_terminal_lineage(tmp_path):
 import json,sqlite3
 private=tmp_path/'private';private.mkdir();packet={'candidate':{'window_id':'w'},'context':{'context_text':'Scott asked Wes about the show.'},'packet_sha256':'a'*64}
 (private/'w.packet.json').write_text(json.dumps(packet))
 db=tmp_path/'dispatch.sqlite';conn=sqlite3.connect(db);conn.row_factory=sqlite3.Row;initialize_dispatch_schema(conn)
 enqueue_task(conn,task_key='gold-speaker-wide-v1:w',task_type='test',payload={})
 lease=acquire_lease(conn,lease_owner='test',lease_seconds=60,task_key_prefix='gold-speaker-wide-v1:')
 fail_attempt_semantically(conn,attempt_id=lease['current_attempt_id'],lease_owner='test',lease_generation=lease['lease_generation'],failure_code='speaker_decision_invalid',failure_detail='mentioned person')
 before=conn.execute('select count(*) from signal_desk_rebuild_attempts').fetchone()[0];conn.close()
 receipt=build_resurrection_receipt(failed_window_id='w',private_root=private,dispatch_path=db)
 assert receipt['resurrection_authorized'] is False and receipt['decision']=='preserve_indeterminable_quarantine'
 conn=sqlite3.connect(db);assert conn.execute('select count(*) from signal_desk_rebuild_attempts').fetchone()[0]==before;conn.close()
