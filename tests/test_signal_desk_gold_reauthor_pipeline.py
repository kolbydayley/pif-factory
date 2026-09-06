from __future__ import annotations
import json
import pytest
from research_factory.signal_desk_gold_reauthor_pipeline import EXPECTED_PROVIDER_CALLS,EXPECTED_TOKENS,RESERVED_TOKEN_CEILING,ReauthorPlanError,validate_overlay

@pytest.fixture
def valid_overlay():
 rows=[{'window_id':f'w{i}','split':'development'} for i in range(9)]
 from research_factory.signal_desk_gold_repair_v4 import _sha_json
 return {'schema_version':'pif_signal_desk_development_source_refreeze_v1','base_manifest_sha256':'base','base_manifest_mutated':False,'validation_or_holdout_content_opened':False,'ready_for_development_gold_authoring':True,'replacement_windows':rows,'replacement_window_count':9,'replacement_windows_digest':_sha_json(rows)}

def test_exact_call_and_token_contract():
 assert EXPECTED_PROVIDER_CALLS==27 and EXPECTED_TOKENS==932588 and RESERVED_TOKEN_CEILING==1_386_000

def test_overlay_allows_only_nine_development_windows(valid_overlay):
 assert len(validate_overlay(valid_overlay,base_manifest_sha256='base'))==9

def test_overlay_rejects_sealed_or_digest_drift(valid_overlay):
 value=json.loads(json.dumps(valid_overlay));value['replacement_windows'][0]['split']='sealed_holdout'
 with pytest.raises(ReauthorPlanError):validate_overlay(value,base_manifest_sha256='base')
 value=json.loads(json.dumps(valid_overlay));value['replacement_windows_digest']='bad'
 with pytest.raises(ReauthorPlanError):validate_overlay(value,base_manifest_sha256='base')
