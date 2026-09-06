from __future__ import annotations
import json,sqlite3
import pytest
from research_factory.signal_desk_gold_source_executor import TASK_PREFIX,load_api_key,prepare_dispatch

def test_key_loads_from_private_file_without_output(monkeypatch,tmp_path):
 path=tmp_path/'key';path.write_text('secret-value\n');monkeypatch.delenv('XAI_API_KEY',raising=False);monkeypatch.setenv('XAI_API_KEY_FILE',str(path));assert load_api_key()=='secret-value'

def test_missing_key_fails_closed(monkeypatch):
 monkeypatch.delenv('XAI_API_KEY',raising=False);monkeypatch.delenv('XAI_API_KEY_FILE',raising=False)
 with pytest.raises(Exception,match='required'):load_api_key()

def test_dispatch_is_resumable_and_deduplicated(tmp_path):
 rows=[{'episode_id':f'e{i}','audio_url_sha256':'a','asr_contract_sha256':'c'} for i in range(5)]
 plan={'paid_network_preflight':{'xai_asr_episodes':rows,'xai_asr_contract_sha256':'c'}};db=tmp_path/'d.sqlite'
 assert prepare_dispatch(dispatch_path=db,plan=plan)=={'created':5}
 assert prepare_dispatch(dispatch_path=db,plan=plan)=={'existing':5}
 connection=sqlite3.connect(db);assert connection.execute("select count(*) from signal_desk_rebuild_tasks where task_key like ?",(TASK_PREFIX+'%',)).fetchone()[0]==5;connection.close()
