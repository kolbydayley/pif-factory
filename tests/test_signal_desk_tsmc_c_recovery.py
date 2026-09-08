import pytest
from research_factory.signal_desk_tsmc_c_recovery import select_supported


def test_matching_id_does_not_approve_changed_content():
    target={'x':{'event_id':'x','claim':'new'}}
    reviewed=[({'event_id':'x','claim':'old'},{'verdict':'supported'},{})]
    with pytest.raises(ValueError,match='incomplete'):select_supported(target,reviewed,key='event_id')


def test_rejected_and_missing_ids_cannot_complete_cover():
    target={'x':{'event_id':'x'}}
    with pytest.raises(ValueError,match='incomplete'):
        select_supported(target,[(target['x'],{'verdict':'material_error'},{})],key='event_id')
    with pytest.raises(ValueError,match='incomplete'):select_supported(target,[],key='event_id')


def test_exact_supported_content_is_covered():
    target={'x':{'event_id':'x','claim':'new'}}
    assert select_supported(target,[(target['x'],{'verdict':'supported'},{'packet':'proof'})],key='event_id')=={'x':{'packet':'proof'}}


def test_actual_full_record_and_lineage_cover():
    import json
    from research_factory.signal_desk_tsmc_c_recovery import recover
    from research_factory.signal_desk_tsmc_c_proposal import prepare
    from scripts import pif_signal_desk_lineage_qualification as lane
    _,_,p=prepare();d=lane.OUT/'calls'/p['window_id']/'C'
    raw=json.loads((d/f"{p['packet_sha256']}.output.json").read_text())
    fixed,proof=recover(raw,p)
    assert len(proof['record_approvals'])==15
    assert len(proof['lineage_approvals'])==29
    assert len(fixed['input_dispositions'])==27 and len(fixed['additions'])==2
    assert not proof['gold_accepted']
